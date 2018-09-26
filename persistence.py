# magneticod - Autonomous BitTorrent DHT crawler and metadata fetcher.
# Copyright (C) 2017  Mert Bora ALPER <bora@boramalper.org>
# Dedicated to Cemile Binay, in whose hands I thrived.
#
# This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
# warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Affero General Public License for more
# details.
#
# You should have received a copy of the GNU Affero General Public License along with this program.  If not, see
# <http://www.gnu.org/licenses/>.
import logging
import pymysql
import time
import typing
import os

from magneticod import bencode
from .constants import PENDING_INFO_HASHES


class Database:

    def __init__(self, database) -> None:
        self.__db_conn = self.__connect(database)

        # We buffer metadata to flush many entries at once, for performance reasons.
        # list of tuple (info_hash, name, total_size, discovered_on)
        self.__pending_metadata = []  # type: typing.List[typing.Tuple[bytes, str, int, int]]
        # list of tuple (info_hash, size, path)
        self.__pending_files = []  # type: typing.List[typing.Tuple[bytes, int, bytes]]

    @staticmethod
    def __connect(database) -> pymysql.Connection:
        db_conn = pymysql.connect("127.0.0.1", "root", "Xdu3702s", "bt_db", charset="utf8")
        # db_conn = pymysql.connect("localhost", "root", "password", "dht", charset="utf8")

        # You need create tables by your self.

        # CREATE TABLE `files` (
        #   `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
        #   `torrent_id` int(11) unsigned NOT NULL,
        #   `size` bigint(20) NOT NULL,
        #   `path` text NOT NULL,
        #   PRIMARY KEY (`id`)
        # ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4

        # CREATE TABLE `torrents` (
        #   `id` bigint(11) unsigned NOT NULL AUTO_INCREMENT,
        #   `info_hash` varchar(64) NOT NULL,
        #   `name` text NOT NULL,
        #   `total_size` bigint(20) NOT NULL DEFAULT '0',
        #   `discovered_on` int(11) NOT NULL DEFAULT '0',
        #   `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
        #   `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        #   `actived_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
        #   `visited_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
        #   PRIMARY KEY (`id`),
        #   KEY `info_hash_index` (`info_hash`)
        # ) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8mb4

        # And you need update this code for connect your mysql server.
        return db_conn

    def add_metadata(self, info_hash: bytes, metadata: bytes) -> bool:
        files = []
        discovered_on = int(time.time())
        info_hash = info_hash.hex()
        try:
            info = bencode.loads(metadata)  # type: dict

            assert b"/" not in info[b"name"]
            name = info[b"name"].decode("utf-8")

            if b"files" in info:  # Multiple File torrent:
                for file in info[b"files"]:
                    assert type(file[b"length"]) is int
                    # Refuse trailing slash in any of the path items
                    assert not any(b"/" in item for item in file[b"path"])
                    path = "/".join(i.decode("utf-8") for i in file[b"path"])
                    files.append([info_hash, file[b"length"], path])
            else:  # Single File torrent:
                assert type(info[b"length"]) is int
                files.append([info_hash, info[b"length"], name])
        # TODO: Make sure this catches ALL, AND ONLY operational errors
        except (bencode.BencodeDecodingError, AssertionError, KeyError, AttributeError, UnicodeDecodeError, TypeError):
            return False

        self.__pending_metadata.append([info_hash, name, sum(f[1] for f in files), discovered_on])
        self.__pending_files += files

        logging.info("Added: `%s`", name)

        # Automatically check if the buffer is full, and commit to the SQLite database if so.
        if len(self.__pending_metadata) >= PENDING_INFO_HASHES:
            self.__commit_metadata()

        return True

    def is_infohash_new(self, info_hash):
        info_hash = info_hash.hex()
        if info_hash in [x[0] for x in self.__pending_metadata]:
            return False
        cur = self.__db_conn.cursor()
        try:
            cur.execute("SELECT count(info_hash) FROM torrents where info_hash = %s;", [info_hash])
            x, = cur.fetchone()
            ise = (x == 0)
            if not ise:
                cur.execute("UPDATE torrents SET actived_at = now() where info_hash = %s;", [info_hash])
                self.__db_conn.commit()
            return ise
        finally:
            cur.close()

    def __commit_metadata(self) -> None:
        cur = self.__db_conn.cursor()

        # Insert metadata
        for metadata in self.__pending_metadata:
            try:
                # Insert torrents
                cur.execute(
                    "INSERT INTO torrents (info_hash, name, total_size, discovered_on) VALUES (%s, %s, %s, %s);",
                    metadata
                )
                self.__db_conn.commit()
            except pymysql.err.IntegrityError as e:
                self.__db_conn.rollback()
                logging.exception("%s metadata is could NOT commit metadata to the database.", metadata[0])
            except:
                self.__db_conn.rollback()
                logging.exception("%s metadata is could NOT commit metadata to the database.", metadata[0])

        # And insert files
        try:
            cur.executemany(
                "INSERT INTO files (torrent_id, size, path) VALUES ((SELECT id FROM torrents WHERE info_hash=%s), %s, %s);",
                self.__pending_files
            )

            self.__db_conn.commit()
            logging.info("%d metadata (%d files) are committed to the database.", len(self.__pending_metadata), len(self.__pending_files))
            self.__pending_metadata.clear()
            self.__pending_files.clear()
        except:
            self.__db_conn.rollback()
            logging.exception("Could NOT commit metadata to the database! (%d metadata are pending)",
                              len(self.__pending_metadata))
        finally:
            cur.close()

    def close(self) -> None:
        if self.__pending_metadata:
            self.__commit_metadata()
        self.__db_conn.close()

