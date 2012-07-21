#!/usr/bin/python
import argparse
import sqlite3
import csv
from datetime import datetime
import os


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


class ExporterDoesNotExist(Exception):
    pass


class Exporter(object):
    def export(self, data, path):
        exporter = self.get_exporter(path)
        return exporter(data, path)

    def get_exporter(self, path):
        filename, file_extension = os.path.splitext(path)
        file_extension = file_extension.lower().replace('.', '')
        exporter = getattr(self, 'export_{0}'.format(file_extension), None)
        if not exporter:
            raise ExporterDoesNotExist('Exporter {0} does not exist'.format(file_extension))
        return exporter

    def export_csv(self, data, path):
        if not data:
            return
        fields = data[0].keys()

        with open(path, 'wb') as f:
            # Write titles
            writer = csv.writer(f)
            writer.writerow(fields)

            writer = csv.DictWriter(f, fields)
            for row in data:
                for k,v in row.items():
                    #print type(v)
                    if isinstance(v, unicode):
                        row[k] = v.encode('utf8')
                    if isinstance(v, float):
                        row[k] = '{0}'.format(v).replace('.', ',')
                writer.writerow(row)


class CoinKeeper(object):
    def __init__(self, db_path, exporter=None):
        assert db_path
        self.connection = sqlite3.connect(db_path)
        self.connection.row_factory = dict_factory
        self.cursor = self.connection.cursor()
        if exporter:
            self.exporter = exporter
        else:
            self.exporter = Exporter

    def get_transactions(self, fields=['Name', 'Note', 'DefaultAmount', 'Icon', 'Date'], order_by='date'):
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

    def export(self, data, path=None):
        if not path:
            path = datetime.now().strftime('%Y-%m-%d.csv')
        exporter = self.exporter()
        exporter.export(data, path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Coin keeper (http://coinkeeper.me/) database export tool')
    parser.add_argument('-d', '--db', type=str, help='database path', default='CoinKeeper2.db3')
    parser.add_argument('-f', '--fields', type=str, help='fields from database to be extracted', nargs='*')
    parser.add_argument('-t', '--target', type=str, help='target file(s)', nargs='*', default=None)
    args = parser.parse_args()

    ck = CoinKeeper(args.db)
    transaction_kw = {}
    if args.fields:
        transaction_kw = {'fields': args.fields}
    data = ck.get_transactions(**transaction_kw)

    if args.target:
        for target in args.target:
            ck.export(data, target)
    else:
        ck.export(data)