import json
import csv
import mysql.connector
import os
from datetime import datetime, time, timedelta

from dotenv import load_dotenv
load_dotenv()

# --- ヘルパー関数群 ---
def convert_value(value, data_type, handle_dash=None):
    """
    指定されたデータ型とハンドリングルールに基づいて値を変換
    """
    # None または空文字列の処理
    if value is None or (isinstance(value, str) and value.strip() == ''):
        if data_type in ['int', 'float']:
            return 0
        elif data_type == 'string' and handle_dash != 'to_null':
            return ''
        return None

    stripped_value = value.strip() if isinstance(value, str) else value

    # ハイフンの処理
    if stripped_value == '-':
        if handle_dash == 'to_zero':
            if data_type == 'int':
                return 0
            elif data_type == 'float':
                return 0.0
            elif data_type == 'time':
                return '00:00:00'
            return None
        elif handle_dash == 'to_null':
            return None
        elif handle_dash == 'to_empty_string':
            return ''
        return None

    # 型変換
    try:
        if data_type == 'int':
            return int(stripped_value)
        elif data_type == 'float':
            return float(stripped_value)
        elif data_type == 'date':
            # YYYY-MM-DDまたはYYYY/MM/DD形式を試行
            try:
                return datetime.strptime(stripped_value, '%Y-%m-%d').date()
            except ValueError:
                return datetime.strptime(stripped_value, '%Y/%m/%d').date()
        elif data_type == 'time':
            # HH:MM または HH:MM:SS 形式を想定
            if ':' in stripped_value:
                parts = stripped_value.split(':')
                if len(parts) == 2: # HH:MM
                    hours = int(parts[0])
                    minutes = int(parts[1])
                    return f"{hours:02d}:{minutes:02d}:00"
                elif len(parts) == 3: # HH:MM:SS
                    return stripped_value
                else:
                    print(f"ERROR: Invalid time format (incorrect number of colons): '{stripped_value}'. Returning None.")
                    return None
            else:
                print(f"ERROR: Invalid time format (no colon found): '{stripped_value}'. Returning None.")
                return None
        elif data_type == 'boolean':
            return stripped_value.lower() in ('true', 'yes', '1', 'はい')
        else: # default to string
            return stripped_value
    except ValueError as ve:
        print(f"ERROR: Value conversion failed (ValueError): Value '{stripped_value}' to {data_type}. Error: {ve}. Returning None.")
        return None
    except Exception as exc:
        print(f"ERROR: Unexpected error during value conversion for '{stripped_value}' to {data_type}. Error: {exc}. Returning None.")
        return None

# --- メインのインポート関数 ---
def import_data_from_config(config_file_path):
    """
    config.jsonの設定に基づいてCSVデータをMySQLにインポート
    """
    conn = None
    cursor = None

    try:
        with open(config_file_path, 'r', encoding='utf-8') as f:
            config = json.load(f)

        db_config = {
            "host": os.getenv("DB_HOST"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "database": os.getenv("DB_DATABASE"),
            "port": int(os.getenv("DB_PORT", 3306))
        }

        # 環境変数の存在チェック
        required_env_vars = ["DB_HOST", "DB_USER", "DB_PASSWORD", "DB_DATABASE"]
        for var in required_env_vars:
            if os.getenv(var) is None:
                print(f"ERROR: Required environment variable '{var}' is not set. Please check your .env file.")
                raise ValueError(f"Required environment variable '{var}' is not set. Please check your .env file.")

        import_settings = config['import_settings']

        csv_file_path = import_settings['csv_file_path']
        table_name = import_settings['table_name']
        csv_encoding = import_settings.get('csv_encoding', 'utf-8')
        skip_header = import_settings.get('skip_header', True)
        
        # CSVから読み込むカラムの定義
        data_columns_map = {col['csv_header_name']: col for col in import_settings['data_columns']}
        db_column_names_from_csv = [col['db_column_name'] for col in import_settings['data_columns']]
        
        # 生成されるカラムの情報を取得
        generated_cols_info = import_settings.get('generated_columns', [])
        
        # 全てのDBカラム名（CSV由来 + 生成されるカラム）の順序付きリスト
        all_db_column_names_ordered = db_column_names_from_csv[:]
        for gen_col in generated_cols_info:
            all_db_column_names_ordered.append(gen_col['db_column_name'])


        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        print("Successfully connected to MySQL database!")

        # 生成されるカラムの値を格納する辞書
        generated_values = {}
        
        # pre_import_action の処理と、生成される値の準備
        pre_action = import_settings.get('pre_import_action')
        if pre_action:
            if pre_action['type'] == 'delete_by_month':
                base_name = os.path.basename(csv_file_path)
                try:
                    parts = base_name.split('_')
                    year_part = parts[pre_action['filename_month_parts_index'][0]]
                    month_part = parts[pre_action['filename_month_parts_index'][1]]
                    
                    # 削除月の初日を生成
                    generated_report_month = datetime(int(year_part), int(month_part), 1).date()
                    
                    # 生成された値を保存
                    month_col_name = pre_action['month_column_in_db']
                    generated_values[month_col_name] = generated_report_month

                    # カラム名がホワイトリストに含まれるかチェック（SQLインジェクション対策）
                    valid_columns = {col['db_column_name'] for col in import_settings['data_columns'] if col['data_type'] == 'date'}
                    if month_col_name not in valid_columns:
                        print(f"WARNING: '{month_col_name}' is not a valid date column for deletion. Skipping delete action.")
                    else:
                        # SQLインジェクションを防ぐため、カラム名を直接文字列に埋め込む
                        delete_sql = f"DELETE FROM {table_name} WHERE {month_col_name} >= %s AND {month_col_name} < %s"
                        # 削除対象月の開始日と次の月の開始日を計算
                        next_month = (generated_report_month.replace(day=28) + timedelta(days=4)).replace(day=1)
                        cursor.execute(delete_sql, (generated_report_month, next_month))
                        conn.commit()
                        print(f"Existing data for {generated_report_month.strftime('%Y-%m')} deleted successfully.")
                except IndexError:
                    print(f"WARNING: Could not determine month from CSV filename '{base_name}' for pre-import action. Check 'filename_month_parts_index' in config.")
                except ValueError:
                    print(f"WARNING: Year/month part in CSV filename '{base_name}' is not a number for pre-import action.")
                except KeyError:
                    print("WARNING: 'month_column_in_db' or 'filename_month_parts_index' not configured for delete_by_month action.")
                except Exception as e:
                    print(f"WARNING: An unexpected error occurred during existing data deletion. Error: {e}")
            elif pre_action['type'] == 'truncate':
                cursor.execute(f"TRUNCATE TABLE {table_name}")
                conn.commit()
                print(f"Table {table_name} truncated successfully.")
            elif pre_action['type'] == 'none':
                print("No existing data deletion action configured ('none'). Be careful of primary key conflicts.")
            else:
                print(f"WARNING: Unknown pre_import_action type: {pre_action['type']}")
        else:
            print("No pre_import_action configured. No existing data will be deleted. Be careful of primary key conflicts.")
        
        # generated_columns の値を生成
        base_name = os.path.basename(csv_file_path) # ループの外で一度だけ取得
        for gen_col_info in generated_cols_info:
            db_col_name = gen_col_info['db_column_name']
            # pre_import_actionで既に生成済みの場合はスキップ
            if db_col_name in generated_values:
                continue 

            gen_rule = gen_col_info.get('generation_rule')
            if gen_rule and gen_rule.get('type') == 'from_filename_month':
                try:
                    parts = base_name.split('_')
                    year_part = parts[gen_rule['filename_month_parts_index'][0]]
                    month_part = parts[gen_rule['filename_month_parts_index'][1]]
                    generated_values[db_col_name] = datetime(int(year_part), int(month_part), 1).date()
                except IndexError:
                    print(f"WARNING: Could not generate '{db_col_name}' from filename '{base_name}'. Check 'filename_month_parts_index' in config. Setting to None.")
                    generated_values[db_col_name] = None
                except ValueError:
                    print(f"WARNING: Year/month part for '{db_col_name}' in filename '{base_name}' is not a number. Setting to None.")
                    generated_values[db_col_name] = None
                except Exception as e:
                    print(f"WARNING: An unexpected error occurred during generation of '{db_col_name}'. Error: {e}. Setting to None.")
                    generated_values[db_col_name] = None
            # 将来的に他の生成ルールが追加される可能性
            else:
                generated_values[db_col_name] = None # 未知の生成ルールはNone


        with open(csv_file_path, 'r', encoding=csv_encoding, errors='ignore') as f:
            csv_reader = csv.reader(f)
            if skip_header:
                csv_header = next(csv_reader)
            # else:
            #     print("WARNING: Skipping header row is set to False. Ensure CSV column order matches DB column order.")
            #     pass

            # CSV列とDB列のマッピングを構築 (CSV由来のカラムのみ)
            csv_idx_to_db_idx_map = []
            for csv_col_idx, csv_col_name in enumerate(csv_header):
                db_col_info = data_columns_map.get(csv_col_name)
                if db_col_info:
                    try:
                        # CSV由来のカラムのDBカラムインデックスは、db_column_names_from_csv 内でのインデックスを使用
                        db_idx = db_column_names_from_csv.index(db_col_info['db_column_name'])
                        csv_idx_to_db_idx_map.append((csv_col_idx, db_idx, db_col_info))
                    except ValueError:
                        print(f"WARNING: DB column '{db_col_info['db_column_name']}' not found in db_column_names_from_csv list. This column will be skipped.")
                # else:
                #     print(f"DEBUG: CSV header '{csv_col_name}' is not mapped in config.json and will be skipped.")

            # INSERT 文の準備 (CSV由来 + 生成されるカラムの全てを含む)
            placeholders = ', '.join(['%s'] * len(all_db_column_names_ordered))
            insert_sql = f"INSERT INTO {table_name} ({', '.join(all_db_column_names_ordered)}) VALUES ({placeholders})"

            data_to_insert = []
            for row_num, raw_row in enumerate(csv_reader, start=2 if skip_header else 1):
                if not raw_row: # 空行はスキップ
                    continue

                # 最終的にDBに挿入するデータを格納するリスト (全DBカラムの数で初期化)
                processed_row_for_db = [None] * len(all_db_column_names_ordered)

                # 生成されたカラムの値を processed_row_for_db にセット
                for gen_col_name, gen_value in generated_values.items():
                    try:
                        # 全DBカラムリストの中でのインデックスを取得
                        overall_db_idx = all_db_column_names_ordered.index(gen_col_name)
                        processed_row_for_db[overall_db_idx] = gen_value
                    except ValueError:
                        print(f"WARNING: Generated column '{gen_col_name}' not found in overall DB column list. This value will be skipped for insertion.")


                # CSV由来のカラムの値を processed_row_for_db にセット
                for csv_col_idx, db_col_idx_in_csv_list, db_col_info in csv_idx_to_db_idx_map:
                    if csv_col_idx >= len(raw_row):
                        print(f"WARNING: CSV row {row_num} is shorter than expected. Missing data for CSV column index {csv_col_idx}. Setting corresponding DB column '{db_col_info['db_column_name']}' to None.")
                        continue

                    csv_value = raw_row[csv_col_idx]
                    db_data_type = db_col_info['data_type']
                    handle_dash = db_col_info.get('handle_dash')
                    # csv_column_name = db_col_info['csv_header_name'] # convert_value から削除されたため不要

                    converted_value = convert_value(csv_value, db_data_type, handle_dash)
                    
                    # db_column_names_from_csv のインデックスから all_db_column_names_ordered のインデックスに変換
                    overall_db_idx_from_csv = all_db_column_names_ordered.index(db_col_info['db_column_name'])
                    processed_row_for_db[overall_db_idx_from_csv] = converted_value

                data_to_insert.append(tuple(processed_row_for_db))

            if data_to_insert:
                try:
                    cursor.executemany(insert_sql, data_to_insert)
                    conn.commit()
                    print(f"{len(data_to_insert)} rows imported successfully into {table_name}!")
                except mysql.connector.Error as err:
                    print(f"ERROR: MySQL batch insert operation failed. Error: {err}")
                    print(f"Affected table: {table_name}")
                    print(f"SQL statement: {insert_sql}")
                    print("Potentially problematic data rows (first few only):")
                    for i, data_row in enumerate(data_to_insert[:5]):
                        print(f"  Row {i+1} (relative to batch): {data_row}")
                    raise
            else:
                print("No data found to import (empty file or only header).")

    except mysql.connector.Error as err:
        print(f"ERROR: MySQL operation failed: {err}")
        if conn and err.errno == 1062:
            print("WARNING: Primary key duplication might have caused some rows to be skipped or errors. Check your data and primary key constraints.")
        if conn:
            conn.rollback()
    except FileNotFoundError as e:
        print(f"ERROR: File not found: {e}")
    except json.JSONDecodeError as e:
        print(f"ERROR: config.json file has a JSON format error: {e}")
    except ValueError as e:
        print(f"CONFIGURATION ERROR: {e}")
    except Exception as e:
        print(f"UNEXPECTED ERROR OCCURRED: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()
            print("MySQL connection closed.")

if __name__ == "__main__":
    CONFIG_FILE = 'config.json'
    print(f"Starting data import process using config file: {CONFIG_FILE}")
    import_data_from_config(CONFIG_FILE)
    print("Data import process finished.")

