from pathlib import Path
from streamlit.testing.v1 import AppTest

app = AppTest.from_file(str(Path(__file__).resolve().parents[1] / "dashboard/app.py"), default_timeout=60).run()
assert not app.exception, app.exception
assert len(app.metric) == 4 and app.metric[0].value == "58,357"
next(w for w in app.selectbox if w.label == "Airport").set_value("BOS").run()
assert not app.exception, app.exception
print("PASS: Streamlit real-demo render and airport interaction")
