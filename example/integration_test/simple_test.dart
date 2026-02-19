import 'dart:convert';
import 'dart:io';

import 'package:integration_test/integration_test.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:flutter_lancedb/flutter_lancedb.dart';
import 'package:path_provider/path_provider.dart';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();
  setUpAll(() async => await RustLib.init());

  test('Can call rust function', () async {
    expect(greet(name: "Tom"), "Hello, Tom!");
  });

  group('LanceDB Integration Tests', () {
    late String dbPath;
    late LanceConnection db;

    setUpAll(() async {
      final dir = await getApplicationDocumentsDirectory();
      dbPath =
          '${dir.path}/lance_test_${DateTime.now().millisecondsSinceEpoch}';
    });

    tearDownAll(() async {
      // Clean up test database
      final dbDir = Directory(dbPath);
      if (await dbDir.exists()) {
        await dbDir.delete(recursive: true);
      }
    });

    test('connect to database', () async {
      db = await connect(uri: dbPath);
      expect(db, isNotNull);
    });

    test('create table with data', () async {
      final data = [
        {
          'id': 1,
          'text': 'hello',
          'vector': [0.1, 0.2, 0.3, 0.4],
        },
        {
          'id': 2,
          'text': 'world',
          'vector': [0.5, 0.6, 0.7, 0.8],
        },
        {
          'id': 3,
          'text': 'test',
          'vector': [0.9, 1.0, 1.1, 1.2],
        },
      ];

      final table = await db.createTable(
        name: 'test_table',
        dataJson: jsonEncode(data),
      );

      expect(table.name(), 'test_table');

      final count = await table.countRows();
      expect(count, 3);
    });

    test('list table names', () async {
      final tableNames = await db.tableNames();
      expect(tableNames, contains('test_table'));
    });

    test('open existing table', () async {
      final table = await db.openTable(name: 'test_table');
      expect(table.name(), 'test_table');
    });

    test('get table schema', () async {
      final table = await db.openTable(name: 'test_table');
      final schema = await table.schema();

      expect(schema.fields.length, greaterThan(0));
      expect(schema.fields.map((f) => f.name), contains('id'));
      expect(schema.fields.map((f) => f.name), contains('text'));
      expect(schema.fields.map((f) => f.name), contains('vector'));
    });

    test('query data', () async {
      final table = await db.openTable(name: 'test_table');
      final result = await table.query(columns: ['id', 'text'], limit: 2);

      expect(result.numRows.toInt(), 2);
      expect(result.columns, containsAll(['id', 'text']));
    });

    test('query with filter', () async {
      final table = await db.openTable(name: 'test_table');
      final result = await table.query(filter: 'id > 1');

      expect(result.numRows.toInt(), 2);
    });

    test('add data to table', () async {
      final table = await db.openTable(name: 'test_table');

      final newData = [
        {
          'id': 4,
          'text': 'added',
          'vector': [1.3, 1.4, 1.5, 1.6],
        },
      ];

      await table.add(dataJson: jsonEncode(newData));

      final count = await table.countRows();
      expect(count, 4);
    });

    test('vector search', () async {
      final table = await db.openTable(name: 'test_table');

      // Search for vectors similar to [0.5, 0.6, 0.7, 0.8]
      final queryVector = [0.5, 0.6, 0.7, 0.8];

      final vq = await table.vectorSearch(
        vector: queryVector.map((e) => e.toDouble()).toList().cast<double>(),
      );
      final result = await vq.limit(limit: 2).execute();

      expect(result.numRows.toInt(), 2);
      // The closest result should be the one with matching vector
      expect(result.rows.first['id'], isNotNull);
    });

    test('vector search with distance type', () async {
      final table = await db.openTable(name: 'test_table');

      final queryVector = [0.5, 0.6, 0.7, 0.8];

      final vq = await table.vectorSearch(
        vector: queryVector.map((e) => e.toDouble()).toList().cast<double>(),
      );
      final result = await vq
          .distanceType(distanceType: DistanceType.cosine)
          .limit(limit: 2)
          .execute();

      expect(result.numRows.toInt(), 2);
    });

    test('delete rows', () async {
      final table = await db.openTable(name: 'test_table');

      await table.delete(predicate: 'id = 4');

      final count = await table.countRows();
      expect(count, 3);
    });

    test('drop table', () async {
      await db.dropTable(name: 'test_table');

      final tableNames = await db.tableNames();
      expect(tableNames, isNot(contains('test_table')));
    });
  });
}
