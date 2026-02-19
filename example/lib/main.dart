import 'dart:convert';
import 'dart:io';

import 'package:flutter/material.dart';
import 'package:flutter_lancedb/flutter_lancedb.dart';
import 'package:path_provider/path_provider.dart';

Future<void> main() async {
  WidgetsFlutterBinding.ensureInitialized();
  await RustLib.init();
  runApp(const MyApp());
}

class MyApp extends StatelessWidget {
  const MyApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(home: const LanceDbDemo());
  }
}

class LanceDbDemo extends StatefulWidget {
  const LanceDbDemo({super.key});

  @override
  State<LanceDbDemo> createState() => _LanceDbDemoState();
}

class _LanceDbDemoState extends State<LanceDbDemo> {
  String _status = 'Ready';
  String _result = '';
  LanceConnection? _db;
  LanceTable? _table;

  Future<String> _getDbPath() async {
    final dir = await getApplicationDocumentsDirectory();
    return '${dir.path}/lancedb_demo';
  }

  Future<void> _connectDb() async {
    try {
      setState(() => _status = 'Connecting...');
      final dbPath = await _getDbPath();

      // Clean up old database for demo
      final dbDir = Directory(dbPath);
      if (await dbDir.exists()) {
        await dbDir.delete(recursive: true);
      }

      _db = await connect(uri: dbPath);
      setState(() {
        _status = 'Connected';
        _result = 'Connected to LanceDB at: $dbPath';
      });
    } catch (e) {
      setState(() {
        _status = 'Error';
        _result = 'Connection failed: $e';
      });
    }
  }

  Future<void> _createTable() async {
    if (_db == null) {
      setState(() => _result = 'Please connect first');
      return;
    }

    try {
      setState(() => _status = 'Creating table...');

      // Sample data with vectors
      final data = [
        {
          'id': 1,
          'text': 'Hello world',
          'vector': List.generate(4, (i) => 0.1 * (i + 1)),
        },
        {
          'id': 2,
          'text': 'Goodbye world',
          'vector': List.generate(4, (i) => 0.2 * (i + 1)),
        },
        {
          'id': 3,
          'text': 'Flutter is awesome',
          'vector': List.generate(4, (i) => 0.3 * (i + 1)),
        },
        {
          'id': 4,
          'text': 'Dart is great',
          'vector': List.generate(4, (i) => 0.4 * (i + 1)),
        },
        {
          'id': 5,
          'text': 'LanceDB rocks',
          'vector': List.generate(4, (i) => 0.5 * (i + 1)),
        },
      ];

      _table = await _db!.createTable(
        name: 'my_vectors',
        dataJson: jsonEncode(data),
      );

      final count = await _table!.countRows();
      final schema = await _table!.schema();

      setState(() {
        _status = 'Table created';
        _result =
            'Created table "${_table!.name()}" with $count rows\n'
            'Schema: ${schema.fields.map((f) => '${f.name}:${f.dataType}').join(', ')}';
      });
    } catch (e) {
      setState(() {
        _status = 'Error';
        _result = 'Create table failed: $e';
      });
    }
  }

  Future<void> _queryData() async {
    if (_table == null) {
      setState(() => _result = 'Please create table first');
      return;
    }

    try {
      setState(() => _status = 'Querying...');

      final result = await _table!.query(columns: ['id', 'text'], limit: 3);

      final buffer = StringBuffer('Query Results (${result.numRows} rows):\n');
      for (final row in result.rows) {
        buffer.writeln('  id: ${row['id']}, text: ${row['text']}');
      }

      setState(() {
        _status = 'Query complete';
        _result = buffer.toString();
      });
    } catch (e) {
      setState(() {
        _status = 'Error';
        _result = 'Query failed: $e';
      });
    }
  }

  Future<void> _vectorSearch() async {
    if (_table == null) {
      setState(() => _result = 'Please create table first');
      return;
    }

    try {
      setState(() => _status = 'Searching...');

      // Search for vectors similar to [0.3, 0.6, 0.9, 1.2]
      final queryVector = [0.3, 0.6, 0.9, 1.2];

      final result = await _table!
          .vectorSearch(vector: queryVector)
          .then((vq) => vq.limit(limit: 3).execute());

      final buffer = StringBuffer(
        'Vector Search Results (${result.numRows} rows):\n',
      );
      for (final row in result.rows) {
        buffer.writeln(
          '  id: ${row['id']}, text: ${row['text']}, _distance: ${row['_distance']}',
        );
      }

      setState(() {
        _status = 'Search complete';
        _result = buffer.toString();
      });
    } catch (e) {
      setState(() {
        _status = 'Error';
        _result = 'Vector search failed: $e';
      });
    }
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(title: const Text('LanceDB Flutter Demo')),
      body: Padding(
        padding: const EdgeInsets.all(16.0),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.stretch,
          children: [
            Text(
              'Status: $_status',
              style: Theme.of(context).textTheme.titleMedium,
            ),
            const SizedBox(height: 8),
            Expanded(
              child: Container(
                padding: const EdgeInsets.all(8),
                decoration: BoxDecoration(
                  border: Border.all(color: Colors.grey),
                  borderRadius: BorderRadius.circular(8),
                ),
                child: SingleChildScrollView(
                  child: Text(
                    _result,
                    style: const TextStyle(fontFamily: 'monospace'),
                  ),
                ),
              ),
            ),
            const SizedBox(height: 16),
            Wrap(
              spacing: 8,
              runSpacing: 8,
              children: [
                ElevatedButton(
                  onPressed: _connectDb,
                  child: const Text('1. Connect'),
                ),
                ElevatedButton(
                  onPressed: _createTable,
                  child: const Text('2. Create Table'),
                ),
                ElevatedButton(
                  onPressed: _queryData,
                  child: const Text('3. Query'),
                ),
                ElevatedButton(
                  onPressed: _vectorSearch,
                  child: const Text('4. Vector Search'),
                ),
              ],
            ),
          ],
        ),
      ),
    );
  }
}
