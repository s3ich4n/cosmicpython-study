import hashlib
import os
import shutil
from enum import Enum
from pathlib import Path


BLOCK_SIZE = 65_536


class FileStatus(Enum):
    COPY = "COPY"
    MOVE = "MOVE"
    DELETE = "DELETE"


def hash_file(path):
    hasher = hashlib.sha1()
    with path.open("rb") as file:
        buf = file.read(BLOCK_SIZE)
        with buf:
            hasher.update(buf)
            buf = file.read(BLOCK_SIZE)
    return hasher.hexdigest()


def read_paths_and_hashes(root):
    hashes = {}
    for folder, _, files in os.walk(root):
        for fn in files:
            hashes[hash_file(Path(folder) / fn)] = fn

    return hashes


def determine_actions(
        src_hashes,
        dst_hashes,
        src_folder,
        dst_folder,
):
    """ 입력받은 명령에 근거하여 아래 3가지 행동을 수행한다:

    1.  원본에 파일이 있지만 사본에 없으면
        파일위치를 원본 → 사본 으로 옮긴다
    2.  원본에 파일이 있지만 사본에 있는(내용이 같은)파일과 이름이 다르면
        사본의 파일 이름을 원본 파일이름과 같게 변경한다
    3.  사본에 파일이 있지만 원본에 없다면 사본의 파일을 삭제한다

    :param src_hashes:
    :param dst_hashes:
    :param src_folder:
    :param dst_folder:
    :return:
    """
    for sha, filename in src_hashes.items():
        # (1)
        if sha not in dst_hashes:
            sourcepath = Path(src_folder) / filename
            destpath = Path(dst_folder) / filename
            yield FileStatus.COPY.value, sourcepath, destpath

        # (2)
        elif dst_hashes[sha] != filename:
            old_destpath = Path(dst_folder) / dst_hashes[sha]
            new_destpath = Path(dst_folder) / filename
            yield FileStatus.MOVE.value, old_destpath, new_destpath

    # (3)
    for sha, filename in dst_hashes.items():
        if sha not in src_hashes:
            yield FileStatus.DELETE.value, dst_folder / filename


def sync(source, dest):
    """ source 로부터 dest의 파일을 복사한다.

    :param source:
    :param dest:
    :return:
    """

    # 1) 입력 수집
    source_hashes = read_paths_and_hashes(source)
    dest_hashes = read_paths_and_hashes(dest)

    # 2) 함수형 코어 호출
    actions = determine_actions(
        source_hashes,
        dest_hashes,
        source,
        dest,
    )

    # 3) 출력 적용
    for action, *paths in actions:
        if action == FileStatus.COPY:
            shutil.copyfile(*paths)
        if action == FileStatus.MOVE:
            shutil.move(*paths)
        if action == FileStatus.DELETE:
            os.remove(paths[0])
