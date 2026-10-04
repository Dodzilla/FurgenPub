import ast,functools,inspect,json,sys,time,urllib.request,argparse
import torch
torch.set_num_threads(2)
from comfy_kitchen.backends.eager import quantization as q
from comfy_kitchen.registry import registry
parser=argparse.ArgumentParser(description="Compare managed eager INT8 policy against installed upstream using synthetic tensors only.")
parser.add_argument('--policy-file',required=True)
parser.add_argument('--cuda',action='store_true')
parser.add_argument('--comfy-url',default='http://127.0.0.1:8188')
args=parser.parse_args()
policy=ast.parse(open(args.policy_file).read())
ns={'functools':functools}
for name in ['bounded_eager_int8_linear','install_eager_int8_memory_policy']:
 node=next(n for n in policy.body if isinstance(n,ast.FunctionDef) and n.name==name)
 exec(compile(ast.Module(body=[node],type_ignores=[]),'<candidate>','exec'),ns)
original=q.int8_linear
wrapped=ns['bounded_eager_int8_linear'](original,chunk_bytes=4096)
passed=0
for device in ['cpu']:
 for dtype in [torch.float16,torch.bfloat16,torch.float32]:
  for bias_present in [False,True]:
   for channel_scale in [False,True]:
    for convrot in [False,True]:
     for shape in [(128,64),(2,64,64)]:
      torch.manual_seed(41)
      x=torch.randn(shape,device=device,dtype=dtype)
      w=torch.randint(-100,101,(128,64),device=device,dtype=torch.int8)
      scale=torch.rand(128 if channel_scale else 1,device=device)*0.02
      bias=torch.randn(128,device=device,dtype=dtype) if bias_present else None
      kw=dict(bias=bias,out_dtype=dtype,convrot=convrot,convrot_groupsize=64)
      old=original(x,w,scale,**kw);new=wrapped(x,w,scale,**kw)
      assert torch.equal(old,new),(dtype,bias_present,channel_scale,convrot,shape,(old-new).abs().max().item())
      passed+=1
# Pass-through branches and errors: no full flattening copy, odd row count, small tensor.
for shape in [(0,64),(31,64),(1,64)]:
 x=torch.randn(shape);w=torch.randint(-100,101,(128,64),dtype=torch.int8);s=torch.tensor([.01])
 try:
  old=original(x,w,s);new=wrapped(x,w,s);assert torch.equal(old,new)
 except Exception as e:
  try:wrapped(x,w,s)
  except Exception as other:assert type(other)==type(e)
  else:raise AssertionError('error not preserved')
 passed+=1
x=torch.randn(64,128).T;assert not x.is_contiguous()
w=torch.randint(-100,101,(128,64),dtype=torch.int8);s=torch.tensor([.01])
assert torch.equal(original(x,w,s),wrapped(x,w,s));passed+=1

for device in ['cpu']:
 for dtype in [torch.float16,torch.bfloat16,torch.float32]:
  for activation in ['none','gelu_tanh','swiglu']:
   x=torch.randn((128,128 if activation=='swiglu' else 64),device=device,dtype=dtype)
   w=torch.randint(-100,101,(128,64),device=device,dtype=torch.int8);s=torch.tensor([.01],device=device)
   assert torch.equal(original(x,w,s,out_dtype=dtype,input_act=activation),wrapped(x,w,s,out_dtype=dtype,input_act=activation))
   passed+=1
print(json.dumps({'cpuParityCases':passed,'signature':str(inspect.signature(original))}))
status=ns['install_eager_int8_memory_policy']();assert status['active'],status
resolved=registry.get_implementation('int8_linear',backend='eager');assert getattr(resolved,'_furgen_bounded_int8',False)
print(json.dumps({'registryBinding':status}))
if not args.cuda: sys.exit(0)
# GPU tests require live Comfy queue empty; no model inference or Comfy submission.
queue=json.load(urllib.request.urlopen(args.comfy_url.rstrip('/')+'/queue',timeout=3))
if queue['queue_running'] or queue['queue_pending']:
 print(json.dumps({'cudaTests':'skipped_busy'}));sys.exit(2)
for dtype in [torch.float16,torch.bfloat16,torch.float32]:
 for convrot in [False,True]:
  x=torch.randn((128,64),device='cuda',dtype=dtype);w=torch.randint(-100,101,(128,64),device='cuda',dtype=torch.int8);s=torch.rand(128,device='cuda')*.02;b=torch.randn(128,device='cuda',dtype=dtype)
  old=original(x,w,s,b,dtype,convrot,64);new=wrapped(x,w,s,b,dtype,convrot,64)
  assert torch.equal(old,new),(dtype,convrot,(old-new).abs().max().item())
  del old,new,x,w,s,b
for dtype in [torch.float16,torch.bfloat16,torch.float32]:
 for activation in ['none','gelu_tanh','swiglu']:
  x=torch.randn((128,128 if activation=='swiglu' else 64),device='cuda',dtype=dtype)
  w=torch.randint(-100,101,(128,64),device='cuda',dtype=torch.int8);s=torch.tensor([.01],device='cuda')
  assert torch.equal(original(x,w,s,out_dtype=dtype,input_act=activation),wrapped(x,w,s,out_dtype=dtype,input_act=activation))
  del x,w,s
print(json.dumps({'cudaParityCases':15}))
# Real custom-op dispatch under eager override; output exceeds production64MiBthreshold.
x=torch.randn((32768,64),device='cuda',dtype=torch.bfloat16);w=torch.randint(-100,101,(2048,64),device='cuda',dtype=torch.int8);s=torch.tensor([.01],device='cuda')
results=[]
for name,fn in [('original',original),('bounded',resolved)]:
 torch.cuda.empty_cache();torch.cuda.synchronize();baseline=torch.cuda.memory_allocated();torch.cuda.reset_peak_memory_stats();start=time.monotonic()
 out=fn(x,w,s);torch.cuda.synchronize();elapsed=time.monotonic()-start
 results.append({'name':name,'peakExtraBytes':torch.cuda.max_memory_allocated()-baseline,'elapsedSec':elapsed})
 if name=='original':expected=out.cpu()
 else:assert torch.equal(expected,out.cpu())
 del out
with registry.use_backend('eager'):
 import comfy_kitchen
 out=comfy_kitchen.int8_linear(x,w,s)
 assert torch.equal(expected,out.cpu())
 del out
assert results[1]['peakExtraBytes'] < results[0]['peakExtraBytes']*.65,results
print(json.dumps({'cudaMemory':results,'customOpParity':'pass'}))
