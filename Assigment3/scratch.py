import importlib
import importlib.util
spec = importlib.util.spec_from_file_location("clientScript", r"C:\Users\markf\OneDrive\Documenten\School\Programming2\Assigment3\WatchDirectory\script1.py")
script = importlib.util.module_from_spec(spec)
spec.loader.exec_module(script)
for d in script.Data('32547504'):
    print(d)