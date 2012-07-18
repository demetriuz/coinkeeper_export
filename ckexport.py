#!/usr/bin/python
import sqlite3
import csv
from datetime import datetime


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def export_csv(data, path):
    data = list(data)
    if not data:
        return
    fields = data[0].keys()
    with open(path, 'wb') as f:
        writer = csv.DictWriter(f, fields)
        for row in data:
            for k,v in row.items():
                v = unicode(v)
                row[k] = v.encode('utf-8')
            writer.writerow(row)


class CoinKeeper(object):
    def __init__(self, db_path='CoinKeeper2.db3'):
        self.connection = sqlite3.connect(db_path)
        self.connection.row_factory = dict_factory
        self.cursor = self.connection.cursor()

    def get_transactions(self, fields=['Note', 'Name', 'DefaultAmount', 'Icon', 'Date'], order_by='date'):
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
        return self.cursor.execute(sql)

    def export(self, data, path=None, exporter=export_csv):
        if not path:
            path = datetime.now().strftime('%Y-%m-%d')
        exporter(data, path)


if __name__ == '__main__':
    ck = CoinKeeper()
    data = ck.get_transactions()
    ck.export(data)