"""Package committed browser artifacts and their source binding."""
from pathlib import Path
import hashlib,json,shutil,subprocess,sys
root=Path(__file__).resolve().parents[1]
out=Path(sys.argv[1]);out.mkdir()
shutil.copytree(root/'docs',out/'site')
files=['wasm/public.cc','wasm/build.sh','src/store.cc','src/records.cc','src/wal.cc','src/wire.cc']
manifest={'source_head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip(),
 'taut_dependency':'a7720b961e0240ecf28e6fd699270dc926c85e57',
 'sources':{p:hashlib.sha256((root/p).read_bytes()).hexdigest() for p in files},
 'assets':{str(p.relative_to(out/'site')):hashlib.sha256(p.read_bytes()).hexdigest() for p in (out/'site').rglob('*') if p.is_file()}}
(out/'source-manifest.json').write_text(json.dumps(manifest,indent=2))
(out/'vercel.json').write_text(json.dumps({'framework':None,'buildCommand':'','outputDirectory':'site'},indent=2))
