
from pathlib import Path
import runpy
runpy.run_path(str(Path(__file__).with_name('app_v3_fixed.py')), run_name='__main__')
