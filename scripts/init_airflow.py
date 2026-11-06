import json
import os
from pathlib import Path

p = Path("/state/simple_auth_manager_passwords.json")
p.parent.mkdir(parents=True, exist_ok=True)
p.write_text(json.dumps({"admin": os.environ["AIRFLOW_ADMIN_PASSWORD"]}), encoding="utf-8")
p.chmod(0o600)
