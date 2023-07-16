from .rust import call_count
import time, os

default_numchan = os.cpu_count()


class ProgressPercent:
    def __init__(self, message):
        self.message = message
        self.percent = 0
    
    def change_message(self, new_message):
        self.message = new_message
        
        self.update()
    
    def progress_percent(self, percent):
        self.percent = percent
        self.update() 
    def finish(self):
        print("")
        
        
    def update(self):
        
        print("\r[%-50.50s] %4.1f%% %s" % ("=" * int(max(0,min(100,self.percent)) / 2)+("*" if self.percent<100 else ""), self.percent, self.message), end="")


def bytes_str(bytes):
    if bytes < 0:
        return "??"
    if bytes < 1024*1.5:
        return "%6db" % (bytes,)
    if bytes < 1024*1024*1.5:
        return "%6.1fkb" % (bytes / 1024.)
    if bytes < 1024*1024*1024*1.5:
        return "%6.1fMb" % (bytes / 1024.0 / 1024.0)
    
    return "%6.1fGb" % (bytes / 1024.0 / 1024.0 / 1024.0)
    
        
class ProgressBytes:
    def __init__(self, message, total_bytes):
        self.message = message
        self.bytes=0
        self.total_bytes=total_bytes
        self.start_time = time.time()
    
    def change_message(self, new_message):
        self.message = new_message
        
        self.update()
    
    def progress_bytes(self, bytes):
        self.bytes = bytes
        self.update() 
    def finish(self):
        print("")
    def update(self):
        ct = time.time()-self.start_time
        rt = (self.total_bytes-self.bytes) / self.bytes * ct if self.bytes > 0  else 0
        cts, rts = "%6.1fs" % ct, "%6.1fs" % rt
        
        progstr = "=" * int(max(0,min(100,100.0*self.bytes/self.total_bytes)) / 2)+("*" if self.bytes<self.total_bytes else "")
        print("\r[%8s] [%-50.50s] %s / %s [%8s rem] %s" % (cts, progstr, bytes_str(self.bytes), bytes_str(self.total_bytes), rts, self.message), end="")


class Messenger:
    def __init__(self):
        self.messages = []
    
    def message(self, message):
        self.messages.append(message)
        print("%s" % message)

    def start_progress_percent(self, message):
        return ProgressPercent(message)
    
    def start_progress_bytes(self, message, total_bytes):
        return ProgressBytes(message,total_bytes)


def iter_tree(tree):
    p=0
    while p is not None:
        q=tree[p]
        if q[2]:
            yield q
        p=tree.next(p)
    
    

messenger = Messenger()
rust.register_messenger(messenger)


def run_count(fname, use_primitive=False, numchan=default_numchan, filter_in=None):
    print(call_count(fname, use_primitive, numchan, filter_in))

class AddBlocks:
    def __init__(self):
        self.bls=[]
    
    def __call__(self, bls):
        self.bls.extend(bls)
    



class LogTimes:
    def __init__(self, msgs=[]):
        
        self.last_time = time.time()
        self.messages=msgs
    def __call__(self, message):
        next_time=time.time()
        self.messages.append((message, next_time-self.last_time))
        self.last_time=next_time
    
    def __str__(self):
        if not self.messages:
            return "LogTimes empty"
        max_len = max(len(a) for a,b in self.messages)
        total = sum(b for a,b in self.messages)
        return "\n".join("%s:%s%s" % (a, " "*(max_len-len(a)), min_sec_str(b)) for a,b in self.messages+[('TOTAL',total)])

def min_sec_str(t):
    if t<90:
        return "     %4.1fs" % t
    mins = int(t/60)
    secs = t - mins*60
    
    return "%4dm%4.1fs" % (mins, secs)
        

def time_op(op, *args, **kwargs):
    st = time.time()
    res = op(*args, **kwargs)
    return time.time()-st, res
    

def run_sortblocks(
    in_filename,
    out_filename,
    timestamp=None,
    qts_filename=None,
    target=40000,
    min_target=None,
    max_qt_level=17,
    numchan=default_numchan):

    lt = LogTimes()
    if not timestamp is None:
        if not isinstance(timestamp, int):
            timestamp = rust.parse_timestamp(timestamp)
    
    if qts_filename is None:
        if not in_filename.endswith(".pbf"):
            raise Exception("unexpected in_filename %s" % in_filename)
        qts_filename = "%s-qts.pbf" % (in_filename[:-4],)
    
    
    tree = rust.prepare_quadtree_tree(numchan, max_qt_level)
    print(tree)
    lt("prepare quadtree tree")
    
    if min_target is None:
        min_target = target // 2
    
    groups = rust.find_tree_groups(tree, target, min_target)
    lt("find tree groups")
    print(groups)
    
    splitat=1500000//target
    limit=30000000 // (groups.num_entries()//splitat)
    
    in_mem = os.stat(in_filename).st_size < 4*1024*1024*1024
    lt.messages += orb.rust.sort_blocks(in_filename, qts_filename, out_filename, groups, numchan, splitat,in_mem,limit,timestamp, False)
    
    
    print(lt)
    
def run_calcqts(in_filename, qts_filename=None, max_qt_level=18, qt_buffer=0.05, mode=None, numchan=default_numchan):
    
    print(LogTimes(rust.run_calcqts(in_filename, qts_filename, max_qt_level, qt_buffer, mode, numchan)))
    



#def read_all_list(readblocks):
#    ab = AddBlocks()
#    readblocks.read_all(ab, 4, False, 32)
#    return ab.bls
    
#rust.ReadFileBlocks.read_all_list = read_all_list

class ReadFileBlocksParallel:
    def __init__(self, prfx, filter=None, timestamp=None, callback_num_blocks=32):
        if isinstance(filter, str):
            if os.path.exists(filter):
                filter = rust.Poly.from_file(filter)
                print("filter: %.50s" % (repr(filter),))
        self.inner = rust.ReadFileBlocksParallel(prfx, filter, timestamp, callback_num_blocks)
        
    def num_blocks(self):
        return self.inner.num_blocks()
    
    def index_at(self, idx):
        return self.inner.index_at(idx)
    
    def fileblocks_at(self, idx):
        return self.inner.fileblocks_at(idx)
        
    def primitive_block_at(self, idx):
        return self.inner.primitive_block_at(idx)
        
    def minimal_block_at(self, idx):
        return self.inner.minimal_block_at(idx)
    
    def read_all(self, callback_func, ids_obj=None, numchan=default_numchan):
        self.inner.read_all(callback_func, ids_obj, numchan)
    
    def read_all_minimal(self, callback_func, numchan=default_numchan):
        self.inner.read_all_minimal(callback_func, numchan)
    
    def read_all_list(self, ids_obj=None, numchan=default_numchan):
        ab = AddBlocks()
        self.inner.read_all(ab, ids_obj, numchan)
        return ab.bls

    def prep_bbox_filter(self, numchan=default_numchan):
        return self.inner.prep_bbox_filter(numchan)
    
    def write_merged(self, outfn, ids_obj=None, numchan=default_numchan):
        self.inner.write_merged(outfn, ids_obj, numchan)
    
    def write_merged_sort(self, outfn, ids_obj=None, inmem=False, numchan=default_numchan):
        self.inner.write_merged_sort(outfn, ids_obj, inmem, numchan)
    
    def __repr__(self):
        return repr(self.inner)
