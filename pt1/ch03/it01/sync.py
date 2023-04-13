import hashlib
import os
import shutil
from pathlib import Path


BLOCK_SIZE = 65_536


def hash_file(path):
    hasher = hashlib.sha1()
    with path.open("rb") as file:
        buf = file.read(BLOCK_SIZE)
        with buf:
            hasher.update(buf)
            buf = file.read(BLOCK_SIZE)
    return hasher.hexdigest()


def sync(source, dest):
    """ source 로부터 dest의 파일을 복사한다.

    1.  원본에 파일이 있지만 사본에 없으면
        파일위치를 원본 → 사본 으로 옮긴다
    2.  원본에 파일이 있지만 사본에 있는(내용이 같은)파일과 이름이 다르면
        사본의 파일 이름을 원본 파일이름과 같게 변경한다
    3.  사본에 파일이 있지만 원본에 없다면 사본의 파일을 삭제한다

    :param source:
    :param dest:
    :return:
    """
    source_hashes = {}

    for folder, _, files in os.walk(source):
        for fn in files:
            source_hashes[hash_file(Path(folder) / fn)] = fn

    seen = set()

    for folder, _, files in os.walk(dest):
        for fn in files:
            dest_path = Path(folder) / fn
            dest_hash = hash_file(dest_path)
            seen.add(dest_hash)

            # (3)
            if dest_hash not in source_hashes:
                dest_path.remove()

            # (2)
            elif dest_hash in source_hashes and fn != source_hashes[dest_hash]:
                shutil.move(dest_path, Path(folder) / source_hashes[dest_hash])

    # (1)
    for src_hash, fn in source_hashes.items():
        if src_hash not in seen:
            shutil.copy(Path(source) / fn, Path(dest) / fn)
