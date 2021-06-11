from .rust import call_count

def run_count(fname, use_primitive=False, numchan=4, filter_in=None):
    print(call_count(fname, use_primitive, numchan, filter_in))
