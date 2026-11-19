"""Synchronize a validated inspection release without copying settings or data."""
import argparse
import hashlib
import json
from datetime import datetime
from pathlib import Path
import shutil
import zipfile

ROOT = Path(__file__).resolve().parents[2]
MANIFEST = ROOT / 'inspection-release-manifest.json'

def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('targets', nargs='+', type=Path)
    parser.add_argument('--verify-only', action='store_true')
    parser.add_argument('--restore-archive', type=Path)
    args = parser.parse_args()
    if args.restore_archive:
        if len(args.targets) != 1:
            raise ValueError('Restore requires one named app directory')
        target = args.targets[0].resolve(strict=True)
        with zipfile.ZipFile(args.restore_archive) as saved:
            for member in saved.infolist():
                destination = (target / member.filename).resolve()
                if not destination.is_relative_to(target):
                    raise ValueError('Unsafe archive path')
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(saved.read(member))
        print('Restored previous app code to ' + str(target))
        return
    manifest = json.loads(MANIFEST.read_text())
    files = manifest['files']
    for relative, expected in files.items():
        source = (ROOT / relative).resolve()
        if not source.is_relative_to(ROOT) or digest(source) != expected:
            raise ValueError('Source release changed: ' + relative)
    stamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    results = []
    for target in args.targets:
        target = target.resolve(strict=True)
        if target == ROOT:
            continue
        if not (target / 'node_app' / 'package.json').is_file() or not (target / 'workflow_db.py').is_file():
            raise ValueError('Not a complete inspection app: ' + str(target))
        changed = []
        for relative in files:
            destination = (target / relative).resolve()
            if not destination.is_relative_to(target):
                raise ValueError('Destination escaped app directory')
            if not destination.exists() or digest(destination) != files[relative]:
                changed.append(relative)
        retired = []
        for relative in manifest.get('retired_files', []):
            destination = (target / relative).resolve()
            if not destination.is_relative_to(target):
                raise ValueError('Retired path escaped app directory')
            if destination.is_file():
                retired.append(relative)
        archive = None
        if not args.verify_only and (changed or retired):
            backup = ROOT / 'work' / 'inspection-sync-backups' / stamp
            backup.mkdir(parents=True, exist_ok=True)
            archive = backup / (target.parent.name + '-' + target.name + '-replaced.zip')
            with zipfile.ZipFile(archive, 'w', zipfile.ZIP_DEFLATED) as saved:
                for relative in changed + retired:
                    destination = target / relative
                    if destination.is_file():
                        saved.write(destination, relative)
            for relative in changed:
                destination = target / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(ROOT / relative, destination)
            for relative in retired:
                (target / relative).unlink()
            shutil.copy2(MANIFEST, target / MANIFEST.name)
        mismatches = [relative for relative, expected in files.items() if not (target / relative).is_file() or digest(target / relative) != expected]
        if args.verify_only:
            mismatches.extend(retired)
        if mismatches:
            raise ValueError('Release verification failed: ' + str(target) + ': ' + ', '.join(mismatches))
        results.append({'folder': str(target), 'verified_files': len(files), 'updated_files': len(changed) if not args.verify_only else 0, 'build': manifest['build'], 'backup': str(archive) if archive else None, 'archived_obsolete_files': len(retired) if not args.verify_only else 0})
    print(json.dumps(results, indent=2))

if __name__ == '__main__':
    main()
