#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
dedupe_md5.py — удаляет дубликаты файлов по MD5, оставляя один файл на каждый уникальный хэш.
Оптимизация: сначала группировка по размеру, затем хэширование только подозрительных групп.
Поддерживает сухой режим, подробный вывод, политику выбора сохраняемого файла и управление симлинками.

Примеры:
  Проба (ничего не удаляется):
    python3 dedupe_md5.py --root . --keep oldest --dry-run -v

  Реальное удаление:
    python3 dedupe_md5.py --root . --keep oldest
"""

import argparse
import hashlib
import os
import sys
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Iterable, List

CHUNK_SIZE = 1024 * 1024  # 1 MiB


def iter_files(root: str, follow_symlinks: bool) -> Iterable[str]:
    """Итерирует файлы в каталоге root.
    Если follow_symlinks=False, симлинки пропускаются (не заходится и не читается их содержимое).
    """
    for dirpath, dirnames, filenames in os.walk(root, followlinks=follow_symlinks):
        for name in filenames:
            path = os.path.join(dirpath, name)
            try:
                # Явно исключаем симлинки, если не хотим по ним ходить
                if not follow_symlinks and os.path.islink(path):
                    continue
                if not os.path.isfile(path):
                    continue
            except OSError:
                continue
            yield path


def md5_of(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def human_bytes(n: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    x = float(n)
    while x >= 1024 and i < len(units) - 1:
        x /= 1024.0
        i += 1
    return f"{x:.2f} {units[i]}"

@dataclass
class Stats:
    scanned: int = 0
    hashed: int = 0
    removed: int = 0
    freed_bytes: int = 0


def choose_keeper(paths: List[str], policy: str) -> str:
    if policy == "first":
        return sorted(paths)[0]  # детерминированно
    elif policy == "oldest":
        return min(paths, key=lambda p: os.stat(p, follow_symlinks=False).st_mtime)
    elif policy == "newest":
        return max(paths, key=lambda p: os.stat(p, follow_symlinks=False).st_mtime)
    else:
        raise ValueError(f"Unknown keep policy: {policy}")


def dedupe(root: str, keep: str, dry_run: bool, verbose: bool, follow_symlinks: bool) -> Stats:
    stats = Stats()
    size_groups: Dict[int, List[str]] = defaultdict(list)

    # 1) Сканируем и группируем по размеру
    for path in iter_files(root, follow_symlinks):
        try:
            st = os.stat(path, follow_symlinks=follow_symlinks)
        except OSError as e:
            if verbose:
                print(f"[WARN] Не удалось stat: {path}: {e}", file=sys.stderr)
            continue
        size_groups[st.st_size].append(path)
        stats.scanned += 1

    # 2) Для групп с одинаковым размером считаем MD5
    for size, paths in size_groups.items():
        if len(paths) < 2:
            continue

        hashes: Dict[str, List[str]] = defaultdict(list)
        for p in paths:
            try:
                h = md5_of(p)
                stats.hashed += 1
            except OSError as e:
                if verbose:
                    print(f"[WARN] Не удалось прочитать: {p}: {e}", file=sys.stderr)
                continue
            hashes[h].append(p)

        # 3) Внутри каждой хэш-группы удаляем дубликаты
        for h, same_hash_paths in hashes.items():
            if len(same_hash_paths) < 2:
                continue

            keeper = choose_keeper(same_hash_paths, keep)
            duplicates = [p for p in same_hash_paths if p != keeper]

            if verbose:
                print(f"[KEEP]  {keeper}  (md5={h})")
                for d in duplicates:
                    print(f"[DUPE]  {d}")

            for d in duplicates:
                try:
                    freed = os.path.getsize(d)
                except OSError:
                    freed = 0
                if dry_run:
                    stats.removed += 1
                    stats.freed_bytes += freed
                else:
                    try:
                        os.remove(d)
                        stats.removed += 1
                        stats.freed_bytes += freed
                    except OSError as e:
                        if verbose:
                            print(f"[WARN] Не удалось удалить {d}: {e}", file=sys.stderr)

    return stats


def parse_args(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Удаляет дубликаты файлов по MD5, оставляя один файл на каждый уникальный хэш."
    )
    p.add_argument("--root", default=".", help="Корневой каталог для рекурсивного обхода (по умолчанию текущий)." )
    p.add_argument(
        "--keep",
        choices=["first", "oldest", "newest"],
        default="first",
        help="Как выбирать, какой файл оставлять внутри группы дубликатов (first|oldest|newest).",
    )
    p.add_argument("--dry-run", action="store_true", help="Ничего не удалять, только показать, что было бы сделано.")
    p.add_argument("--verbose", "-v", action="store_true", help="Подробный вывод.")
    p.add_argument(
        "--follow-symlinks",
        action="store_true",
        help="Следовать симлинкам (по умолчанию — нет). Будьте осторожны: может привести к циклам.",
    )
    return p.parse_args(argv)


def main(argv: List[str]) -> int:
    args = parse_args(argv)
    root = os.path.abspath(args.root)

    if not os.path.isdir(root):
        print(f"[ERR] Не найден каталог: {root}", file=sys.stderr)
        return 2

    if args.verbose:
        print(
            f"[INFO] Старт. Каталог: {root}, keep={args.keep}, dry_run={args.dry_run}, follow_symlinks={args.follow_symlinks}"
        )

    stats = dedupe(
        root=root,
        keep=args.keep,
        dry_run=args.dry_run,
        verbose=args.verbose,
        follow_symlinks=args.follow_symlinks,
    )

    print(
        f"Готово. Просканировано файлов: {stats.scanned}, вычислено MD5: {stats.hashed}, "
        f"удалено дубликатов: {stats.removed}, освобождено: {human_bytes(stats.freed_bytes)}"
    )
    if args.dry_run:
        print("(Сухой режим: ничего не удалено.)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
