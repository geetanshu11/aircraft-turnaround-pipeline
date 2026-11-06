"""Portable bundles and safe checkpoint recovery for an ephemeral Colab runtime."""
from pathlib import Path, PurePosixPath
import json
import os
import shutil
import stat
import zipfile
from aircraft_core import BASE, sha256


def make_checkpoint(root, output):
    root,output=Path(root).resolve(),Path(output).resolve()
    if output.is_relative_to(root):raise ValueError('Checkpoint must be outside the runtime')
    if list(root.glob('*.wal')):raise ValueError('Close the warehouse before checkpointing')
    output.parent.mkdir(parents=True,exist_ok=True)
    temp=output.with_suffix('.partial')
    with zipfile.ZipFile(temp,'w',zipfile.ZIP_DEFLATED,compresslevel=3,allowZip64=True) as z:
        for p in sorted(root.rglob('*')):
            if not p.is_file() or p.is_symlink() or p.name.endswith(('.lock','.partial')) or 'staging' in p.relative_to(root).parts:continue
            z.write(p,p.relative_to(root).as_posix())
    os.replace(temp,output)
    return {'path':str(output),'bytes':output.stat().st_size,'sha256':sha256(output)}


def safe_restore(archive, destination):
    destination=Path(destination).resolve()
    if destination.exists() and any(destination.iterdir()):raise ValueError('Restore needs an empty directory')
    with zipfile.ZipFile(archive) as z:
        if sum(i.file_size for i in z.infolist())>12*1024**3:raise ValueError('Checkpoint exceeds 12 GiB expanded limit')
        seen=set()
        for info in z.infolist():
            p=PurePosixPath(info.filename)
            target=(destination/str(p)).resolve()
            if p.is_absolute() or '..' in p.parts or '\\' in info.filename or ':' in info.filename or not target.is_relative_to(destination):
                raise ValueError('Unsafe checkpoint member')
            if target in seen or stat.S_ISLNK(info.external_attr>>16):raise ValueError('Duplicate path or symbolic link')
            seen.add(target)
        if z.testzip() is not None:raise ValueError('Checkpoint CRC failure')
        destination.mkdir(parents=True,exist_ok=True)
        z.extractall(destination)
    return destination


def copy_verified(source,destination):
    source,destination=Path(source),Path(destination)
    destination.parent.mkdir(parents=True,exist_ok=True)
    temp=destination.with_suffix('.partial')
    shutil.copyfile(source,temp)
    if sha256(source)!=sha256(temp):raise ValueError('Checkpoint copy checksum mismatch')
    os.replace(temp,destination)


def export_project(cfg, publication_dir, output):
    # My small results bundle includes the code, demo and current gold outputs.
    # The optional full checkpoint keeps raw data and the complete warehouse too.
    output=Path(output).resolve();publication_dir=Path(publication_dir)
    if output.is_relative_to(cfg.root):raise ValueError('Export ZIP must be outside runtime')
    with zipfile.ZipFile(output,'w',zipfile.ZIP_DEFLATED,compresslevel=3,allowZip64=True) as z:
        for pattern in ['*.py','*.md','*.txt','*.ini','Dockerfile','.gitignore','.dockerignore','sql/**/*','tests/*.py','assets/*','demo/*','.github/workflows/*']:
            for p in sorted(BASE.glob(pattern)):
                if p.is_file():z.write(p,'project/'+p.relative_to(BASE).as_posix())
        for p in sorted(publication_dir.glob('*')):
            if p.is_file():z.write(p,'verified_results/'+p.name)
        for p in sorted((cfg.root/'logs').glob('*')):
            if p.is_file():z.write(p,'run_logs/'+p.name)
    return {'path':str(output),'bytes':output.stat().st_size,'sha256':sha256(output)}
