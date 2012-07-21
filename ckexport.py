#!/usr/bin/python
import argparse
import sqlite3
import csv
from datetime import datetime
import tempfile
import os
from itertools import groupby


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


class ExporterDoesNotExist(Exception):
    pass


class Exporter(object):
    def export(self, data, path, **kw):
        exporter = self.get_exporter(path)
        return exporter(data, path, **kw)

    def get_exporter(self, path):
        filename, file_extension = os.path.splitext(path)
        file_extension = file_extension.lower().replace('.', '')
        exporter = getattr(self, 'export_{0}'.format(file_extension), None)
        if not exporter:
            raise ExporterDoesNotExist('Exporter {0} does not exist'.format(file_extension))
        return exporter

    def export_csv(self, data, path, fields=None):
        if not data:
            return

        with open(path, 'wb') as f:
            list_writer = csv.writer(f)
            # Write titles
            if fields:
                list_writer.writerow(fields)
            else:
                for row in data:
                    if isinstance(row, dict):
                        list_writer.writerow(row.keys())
                        break

            # Write rows
            for row in data:
                if isinstance(row, dict):
                    dict_writer = csv.DictWriter(f, fields or row.keys())
                    for k,v in row.items():
                        if isinstance(v, unicode):
                            row[k] = v.encode('utf8')
                        if isinstance(v, float):
                            row[k] = '{0}'.format(v).replace('.', ',')
                    dict_writer.writerow(row)
                if isinstance(row, unicode):
                    list_writer.writerow([''])


class Grouper(object):
    def __init__(self, data, groupby='date'):
        self.groupby = groupby
        self.data = data

    def group(self):
        grouper = getattr(self, 'groupby_{0}'.format(self.groupby), None)
        return grouper(self.data)

    def groupby_date(self, data):
        grouped_data = []
        for key, group in groupby(data, lambda x: x['Date'].split(' ')[0]):
            grouped_data.append(key)
            for row in group:
                grouped_data.append(row)
        return grouped_data


class CoinKeeper(object):
    def __init__(self, db_path, exporter=None, grouper=Grouper):
        assert db_path
        self.connection = sqlite3.connect(db_path)
        self.connection.row_factory = dict_factory
        self.cursor = self.connection.cursor()
        self.grouper = grouper
        if exporter:
            self.exporter = exporter
        else:
            self.exporter = Exporter

    def get_transactions(self, fields=['*'], order_by='date'):
        fields = ','.join(fields)
        sql = """
            SELECT
                {fields}
            FROM
                (
                    "Transaction" as t
                    LEFT JOIN Category as c ON t.DestinationUid = c.Uid
                )AS JoinedTable
            WHERE
                JoinedTable.Deleted = 0 AND JoinedTable.Virtual = 0
            ORDER BY
                {order_by}
        """.format(**locals())
        return list(self.cursor.execute(sql))

    def export(self, fields=None, path=None):
        try:
            data = self.get_transactions(fields)
        finally:
            self.connection.close()

        if self.grouper:
            grouper = self.grouper(data)
            data = grouper.group()

        if not path:
            path = datetime.now().strftime('%Y-%m-%d.csv')
        exporter = self.exporter()
        exporter.export(data, path, fields=fields)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Coin keeper (http://coinkeeper.me/) database export tool')
    parser.add_argument('-f', '--fields', type=str, help='fields from database to be extracted', nargs='*', default=['Date', 'Name', 'DefaultAmount', 'Note', 'Icon'])
    parser.add_argument('-t', '--target', type=str, help='target file(s)', nargs='*', default=None)
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-d', '--db', type=str, help='database path', default='CoinKeeper2.db3')
    group.add_argument('--ios', type=bool, const=True, nargs='?')

    args = parser.parse_args()

    def do_export(ck, args):
        if args.target:
            for target in args.target:
                ck.export(target, fields=args.fields)
        else:
            ck.export(fields=args.fields)

    if args.ios:
        from iconnector import ifuse_connect, ifuse_disconnect
        mnt_path = tempfile.mkdtemp()
        db_path = os.path.join(mnt_path, 'CoinKeeper2.db3')
        try:
            ifuse_connect(mnt_path=mnt_path, app_id='com.i-free.coinkeeper')
            ck = CoinKeeper(db_path)
            do_export(ck, args)
        finally:
            ifuse_disconnect(mnt_path)
    else:
        ck = CoinKeeper(args.db)
        do_export(ck, args)