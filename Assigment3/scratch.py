import importlib
import importlib.util
spec = importlib.util.spec_from_file_location("clientScript", r"WatchDirectory/script1.py")
script = importlib.util.module_from_spec(spec)
spec.loader.exec_module(script)
for d in script.Data('32547504'):
    print(d)