"""
FDI0080_tblExportEquipmentWorkToMaster.py

処理名:
    TBL出力（一時DB→設備データ管理マスタDB）

概要:
    一時DBから取得したデータに管理用ID等の属性を追加し、設備データマスタDBに登録する。一時DBのデータは削除する。

実行コマンド形式:
    python3 [バッチ格納先パス]/FDI0080_tblExportEquipmentWorkToMaster.py
    --import_id=[起動パラメータ.取込ID]
"""

import argparse
import traceback
from datetime import datetime

from core.config_reader import read_config
from core.database import Database
from core.logger import LogManager
from core.message import get_message
from core.secretProperties import SecretPropertiesSingleton
from core.validations import Validations
from util.getImportManagementTableName import get_import_management_table_name
from util.updateImportManagement import update_import_management

log_manager = LogManager()
logger = log_manager.get_logger("FDI0080_TBL出力（一時DB→設備データ管理マスタDB）")
db_connection = Database.get_mstdb_connection(logger)
config = read_config(logger)


def get_secret_props(logger):
    """AWS Secrets Managerからシークレット情報を取得"""
    config = read_config(logger)
    secret_name = config["aws"]["secret_name"]
    return SecretPropertiesSingleton(secret_name, config, logger)


secret_props = get_secret_props(logger)
DB_MST_SCHEMA = secret_props.get("db_mst_schema")
DB_FAC_SCHEMA = secret_props.get("db_fac_schema")
DB_WORK_SCHEMA = secret_props.get("db_work_schema")

import_id_list = []


# 起動パラメータを受け取る関数
def parse_args():
    try:
        # 完全一致のみ許可
        parser = argparse.ArgumentParser(allow_abbrev=False, exit_on_error=False)
        parser.add_argument("--import_id", required=False)
        return parser.parse_args()
    except Exception as e:
        # コマンドライン引数の解析に失敗した場合
        logger.error("BPE0037", str(e.message))
        logger.process_error_end()


# 入力値チェック
def validate_import_id(import_id_list):
    valid_import_id_list = []
    error_detail = None

    for proc_import_id in import_id_list:

        # 必須パラメータチェック
        if not proc_import_id:
            # 必須チェックのエラーの場合、5.取込管理テーブルの更新を行わない
            logger.error("BPE0018", "取込ID")
            logger.process_error_end()

        # 自然数チェック
        if not Validations.is_natural_num(proc_import_id):
            if not error_detail:
                logger.error("BPE0019", "取込ID", proc_import_id)
                error_detail = get_message("BPE0019").format("取込ID", proc_import_id)
            continue

        valid_import_id_list.append(proc_import_id)

        # 自然数チェックのエラーが発生した場合、正常な取込IDのみ5.取込管理テーブルの更新を行う
        if error_detail:
            update_import_management_all(valid_import_id_list, error_detail, True)


# 設備データ管理マスタDBと一時DBの存在チェック
def check_db_existence(import_id, work_table_name, fac_data_master_table_name):

    error_detail = None

    # 一時DBの存在チェック（pg_tables使用）
    query_work = (
        f"SELECT EXISTS (SELECT 1 FROM pg_tables "
        f"WHERE schemaname = '{DB_WORK_SCHEMA}' AND tablename = %s)"
    )
    row_work = Database.execute_query(
        db_connection, logger, query_work, (work_table_name,), fetchone=True
    )
    if not row_work:
        logger.error("BPE0043", import_id, work_table_name)
        error_detail = get_message("BPE0043").format(import_id, work_table_name)
        return False, error_detail

    # 設備データ管理マスタDBの存在チェック（pg_tables使用）
    query_master = (
        f"SELECT EXISTS (SELECT 1 FROM pg_tables "
        f"WHERE schemaname = '{DB_FAC_SCHEMA}' AND tablename = %s)"
    )
    row_master = Database.execute_query(
        db_connection,
        logger,
        query_master,
        (fac_data_master_table_name,),
        fetchone=True,
    )
    if not row_master:
        logger.error("BPE0043", import_id, fac_data_master_table_name)
        error_detail = get_message("BPE0043").format(
            import_id, fac_data_master_table_name
        )
        return False, error_detail
    return True, error_detail


# 設備データ管理マスタＤＢの既存データ削除
def truncate_fac_data_master_table(proc_table_info):

    # 設備データ管理マスタＤＢの既存データ削除、シーケンスリセット
    query = (
        f"TRUNCATE TABLE {DB_FAC_SCHEMA}."
        f"{proc_table_info['fac_data_master_table_name']} "
        "RESTART IDENTITY"
    )
    try:
        Database.execute_query_no_commit(
            db_connection, logger, query, raise_exception=True
        )
    except Exception:
        logger.error(
            "BPE0044",
            "削除",
            proc_table_info["import_id"],
            proc_table_info["fac_data_master_table_name"],
        )
        error_detail = get_message("BPE0044").format(
            "削除",
            proc_table_info["import_id"],
            proc_table_info["fac_data_master_table_name"],
        )
        # 取込管理テーブルエラー更新（全レコード）
        update_import_management_all(import_id_list, error_detail, True)


#   一時DBのデータに属性を追加し、設備データ管理マスタＤＢへ登録
def insert_fac_data_master_table(proc_table_info):

    # [設備データ管理マスタテーブル物理カラム名一覧]を取得
    fac_data_master_table_physical_columns = []

    query = (
        "SELECT column_name "
        "FROM information_schema.columns "
        f"WHERE table_schema = '{DB_FAC_SCHEMA}' AND table_name = %s "
    )

    result = Database.execute_query(
        db_connection,
        logger,
        query,
        (proc_table_info["fac_data_master_table_name"],),
        fetchall=True,
    )
    for row in result:
        fac_data_master_table_physical_columns.append(row[0])

    # [一時テーブル物理カラム名一覧]を取得
    work_table_physical_columns = []

    query = (
        "SELECT column_name "
        "FROM information_schema.columns "
        f"WHERE table_schema = '{DB_WORK_SCHEMA}' AND table_name = %s "
    )

    result = Database.execute_query(
        db_connection,
        logger,
        query,
        (proc_table_info["work_table_name"],),
        fetchall=True,
    )
    for row in result:
        work_table_physical_columns.append(row[0])

    # 一時テーブルの情報をすべて取得
    work_table_record = []
    query = f"SELECT * FROM {DB_WORK_SCHEMA}.{proc_table_info['work_table_name']}"
    result = Database.execute_query(
        db_connection,
        logger,
        query,
        fetchall=True,
    )
    for row in result:
        work_table_record.append(row)

    # シークレット設定値（バッチ）から[文字列⇒数値型変換カラム一覧]を取得し配列に格納する
    str_to_num_columns = []
    str_to_num_columns = secret_props.get("char_to_numeric_columns").split(",")

    # 一時テーブル名からプレフィックスのwork_を取り除く
    prefix_mg_id = None
    prefix_mg_id = proc_table_info["work_table_name"].replace("work_", "")

    for idx, record in enumerate(work_table_record, start=1):
        work_table = dict(zip(work_table_physical_columns, record))

        # 一時テーブル物理カラム名が[文字列⇒数値型変換カラム一覧]に含まれている場合、カラム値を数値型に変換
        for key in work_table.keys():
            if key in str_to_num_columns:
                if work_table[key] is None:
                    work_table[key] = " "
                work_table[key] = f"NULLIF('{work_table[key]}', ' ')::NUMERIC"

        # 任意項目1～5を結合してひとつにまとめる
        for key in [
            "opattr_1",
            "opattr_2",
            "opattr_3",
            "opattr_4",
            "opattr_5",
        ]:
            if work_table[key] is None:
                work_table[key] = " "
        opt_attr = (
            f"(COALESCE(NULLIF('{work_table['opattr_1']}', ' '), '{{}}')::jsonb || "
            f"COALESCE(NULLIF('{work_table['opattr_2']}', ' '), '{{}}')::jsonb || "
            f"COALESCE(NULLIF('{work_table['opattr_3']}', ' '), '{{}}')::jsonb || "
            f"COALESCE(NULLIF('{work_table['opattr_4']}', ' '), '{{}}')::jsonb || "
            f"COALESCE(NULLIF('{work_table['opattr_5']}', ' '), '{{}}')::jsonb)::text"
        )
        Values = []
        for col in fac_data_master_table_physical_columns:
            if col == "id":
                Values.append(str(idx))
            elif col == "mg_id":
                Values.append(f"'{prefix_mg_id}_{idx}'")
            elif col == "opt_attr":
                Values.append(opt_attr)
            elif col == "created_by":
                Values.append("'system'")
            elif col == "created_at":
                Values.append("NOW()")
            elif col in work_table.keys():
                # 一時テーブルに存在し、かつ文字列⇒数値型変換カラムの場合
                if col in str_to_num_columns and work_table[col].startswith("NULLIF("):
                    Values.append(f"{work_table[col]}")
                # 一時テーブルに存在するがNULLの場合
                elif work_table[col] is None:
                    Values.append("NULL")
                # 一時テーブルに存在し、かつ文字列⇒数値型変換カラムでない場合
                else:
                    Values.append(f"'{work_table[col]}'")
            # 上記以外で一時テーブルに存在しない場合
            else:
                Values.append("NULL")

        query = (
            f"INSERT INTO {DB_FAC_SCHEMA}."
            f"{proc_table_info['fac_data_master_table_name']} "
            f"({', '.join(fac_data_master_table_physical_columns)}) "
            f"VALUES ({', '.join(Values)})"
        )

        try:
            Database.execute_query_no_commit(
                db_connection, logger, query, raise_exception=True
            )
        except Exception:
            logger.error(
                "BPE0044",
                "登録",
                proc_table_info["import_id"],
                proc_table_info["fac_data_master_table_name"],
            )
            error_detail = get_message("BPE0044").format(
                "登録",
                proc_table_info["import_id"],
                proc_table_info["fac_data_master_table_name"],
            )
            # 取込管理テーブルエラー更新（全レコード）
            update_import_management_all(import_id_list, error_detail, True)


# 取込管理テーブル更新（全レコード）
def update_import_management_all(
    update_import_id_list, error_detail=None, errorFlg=False
):

    # 異常時
    if errorFlg:
        for proc_import_id in update_import_id_list:
            update_import_management(
                db_connection,
                logger,
                proc_import_id,
                "94",
                error_detail,
                None,
                None,
                None,
            )
        logger.process_error_end()
    # 正常時
    else:
        for proc_import_id in update_import_id_list:
            update_import_management(
                db_connection,
                logger,
                proc_import_id,
                "40",
                error_detail,
                None,
                None,
                datetime.now(),
            )


# 一時DBのテーブル削除
def drop_work_table(import_table_info):
    for table_info in import_table_info:
        query = f"DROP TABLE IF EXISTS {DB_WORK_SCHEMA}.{table_info['work_table_name']}"
        try:
            Database.execute_query(db_connection, logger, query, raise_exception=True)
        except Exception:
            logger.warning("BPW0019", "削除", table_info["work_table_name"])


# メイン処理
# TBL出力（一時DB→設備データ管理マスタDB）
def main():
    try:

        # 開始ログ出力
        logger.process_start()

        # 起動パラメータ取得
        import_id_param = parse_args()

        # 1. 入力値チェック
        global import_id_list
        import_id_list = import_id_param.import_id.split(",")
        validate_import_id(import_id_list)

        import_table_info = []
        for proc_import_id in import_id_list:
            # 2. 取込管理内テーブル名取得
            proc_table_names = get_import_management_table_name(
                db_connection, logger, proc_import_id
            )
            if proc_table_names["work_table_name"]:
                proc_work_table_name = proc_table_names["work_table_name"]
                proc_fac_data_master_table_name = proc_table_names[
                    "fac_data_master_table_name"
                ]
            else:
                # 取込管理テーブルエラー更新（全レコード）
                update_import_management_all(
                    import_id_list,
                    proc_table_names["error_detail"],
                    True,
                )

            # 3. 設備データ管理マスタDBと一時DBの存在チェック
            checkFlg = True
            checkFlg, error_detail = check_db_existence(
                proc_import_id,
                proc_work_table_name,
                proc_fac_data_master_table_name,
            )
            if not checkFlg:
                update_import_management_all(import_id_list, error_detail, True)
            else:
                update_import_management(
                    db_connection,
                    logger,
                    proc_import_id,
                    None,
                    None,
                    None,
                    datetime.now(),
                    None,
                )

            # 取込ID, 一時テーブル名、設備データ管理マスタテーブル名をリストに追加
            import_table_info.append(
                {
                    "import_id": proc_import_id,
                    "work_table_name": proc_work_table_name,
                    "fac_data_master_table_name": proc_fac_data_master_table_name,
                }
            )

        # 4. 設備データ管理マスタＤＢ登録
        for proc_table_info in import_table_info:

            # 4-1.設備データ管理マスタＤＢの既存データ削除
            truncate_fac_data_master_table(proc_table_info)

            # # 4-2.一時ＤＢのデータに属性を追加し、設備データ管理マスタＤＢへ登録
            insert_fac_data_master_table(proc_table_info)

        db_connection.commit()

        # 5. 取込管理テーブルの更新
        update_import_management_all(import_id_list)

        # 6.一時DBのテーブル削除
        drop_work_table(import_table_info)

        # 正常終了ログ出力
        logger.process_normal_end()

    except Exception:
        logger.error("BPE0009", traceback.format_exc())
        logger.process_error_end()


if __name__ == "__main__":
    main()
