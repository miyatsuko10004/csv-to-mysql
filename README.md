# csv-to-mysql
csvファイルのデータをmysqlに入れるpythonスクリプト

## .env記載
```
DB_HOST=your_host
DB_USER=your_user
DB_PASSWORD=your_password
DB_DATABASE=your_database_name
DB_PORT=3306
```
## conifg.json記載

```
{
  "import_settings": {
    "csv_file_path": "C:\\Users\\your_name\\Desktop\\somewhere\\example_sample_2025_07.csv",
    "table_name": "your_mysql_table_name",
    "csv_encoding": "utf-8", -- 日本語の場合、cp932
    "skip_header": true, -- 1行目をヘッダーとしてスキップするか？
    "pre_import_action": {
        "type": "delete_by_month",
        "filename_month_parts_index": [2, 3],
        "month_column_in_db": "clock_date"
    },
    "data_columns": [
      {
        "csv_header_name": "社員番号",
        "db_column_name": "employee_id",
        "data_type": "int",
        "handle_dash": "to_zero"
      },
      {
        "csv_header_name": "苗字",
        "db_column_name": "last_name",
        "data_type": "string",
        "handle_dash": "to_empty_string"
      },
      {
        "csv_header_name": "名前",
        "db_column_name": "first_name",
        "data_type": "string",
        "handle_dash": "to_empty_string"
      },
      {
        "csv_header_name": "誕生日",
        "db_column_name": "date_of_birth",
        "data_type": "date",
        "handle_dash": "to_null"
      },
      --------- 以下同様にカラムの数だけ追加 ------
    ],
    "generated_columns": []
  }
}

```

### 全体構造
config.jsonは、import_settingsという一つの主要なオブジェクトで構成されます。このオブジェクトの中に、インポート処理に関するすべての設定を記述します。

```
{
  "import_settings": {
    // ここに設定を記述
  }
}
```

### 共通設定
import_settingsの冒頭で、インポート処理全体の基本設定を行います。

- "csv_file_path": 必須。インポートしたいCSVファイルへのパスを指定します。Windowsのパスでは、バックスラッシュ\を\\とエスケープする必要があります。
- "table_name": 必須。データを挿入するMySQLのテーブル名を指定します。
- "csv_encoding": 必須。CSVファイルの文字コードを指定します。日本語のCSVでは"utf-8"または**"cp932"**が一般的です。
- "skip_header": 必須。CSVファイルの1行目をヘッダーとしてスキップするかどうかをtrueかfalseで指定します。通常はtrueです。

### 事前処理 (pre_import_action)
これは、データインポート前に実行されるアクションを定義します。これにより、データの重複挿入を防ぐことができます。

- "type"で実行するアクションを指定します。
- "none": 何も実行しません。
- "truncate": テーブルの全データを削除します。
- "delete_by_month": CSVファイル名から年月を抽出し、その月のデータをテーブルから削除します。
- "delete_by_month"使用時の設定
- "type": "delete_by_month"を選択した場合、以下の2つの設定が追加で必要です。
- "filename_month_parts_index": CSVファイル名をアンダースコア_で区切った際の、年と月の部分のインデックスをリストで指定します。例えば、report_2025_07.csvというファイル名なら[1, 2]となります。
- "month_column_in_db": 削除の条件として使用する、データベースの日付型カラム名を指定します。

```
"pre_import_action": {
  "type": "delete_by_month",
  "filename_month_parts_index": [3, 4],
  "month_column_in_db": "clock_date"
}
```

### カラムのマッピング (data_columns)
このセクションでは、CSVの各列とデータベースのカラムをどのように関連付けるかを設定します。これは配列になっており、各要素が1つのカラムに対応します。

- "csv_header_name": 必須。CSVファイルのヘッダーにある列名を正確に記述します。
- "db_column_name": 必須。対応するデータベースのカラム名を記述します。
- "data_type": 必須。データの型を"string", "int", "float", "date", "time", "boolean"から選択します。
- "handle_dash": CSV内のハイフン-の処理方法を指定します。
- "to_null": NULLを挿入します。（NOT NULL制約のないカラム向け）
- "to_zero": 0または00:00:00を挿入します。（数値型・時間型カラム向け）
- "to_empty_string": 空文字列""を挿入します。（文字列型カラム向け）

```
"data_columns": [
  {
    "csv_header_name": "従業員番号",
    "db_column_name": "employee_id",
    "data_type": "int",
    "handle_dash": "to_zero"
  },
  {
    "csv_header_name": "入社年月日",
    "db_column_name": "date_of_hire",
    "data_type": "date",
    "handle_dash": "to_null"
  }
```

### 生成されるカラム (generated_columns)
CSVに存在しないが、インポート時に自動生成して挿入したいカラムがある場合に設定します。

"db_column_name": 必須。値を挿入するデータベースのカラム名を記述します。

"generation_rule": 必須。値の生成ルールを定義します。現在のバージョンでは、"from_filename_month"のみがサポートされています。

"from_filename_month"使用時の設定
"type": "from_filename_month": CSVファイル名から年月を抽出し、その月の初日をDATE型で生成します。

"filename_month_parts_index": pre_import_actionと同様に、年と月のインデックスを指定します。

```
"generated_columns": [
  {
    "db_column_name": "report_month",
    "generation_rule": {
      "type": "from_filename_month",
      "filename_month_parts_index": [3, 4]
    }
  }
]
```
