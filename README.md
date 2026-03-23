# 概要
本リポジトリでは、下記の機能を提供します。
| 機能名                                   | 機能概要                                                                                                       |
| ---------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| TBL出力（一時DB→設備データ管理マスタDB） | 一時DBから取得したデータに管理用ID等の属性を追加し、設備データマスタDBに登録する。一時DBのデータは削除する。 |

# フォルダ構成
```
└── App
    ├── common              // 共通処理モジュール（依存リポジトリからコピー）
    ├── config              // 設定ファイル（依存リポジトリからコピー）
    ├── core                // 基盤機能モジュール（依存リポジトリからコピー）
    ├── functions           // 機能実装（本機能）
    └── util                // 各種ユーティリティモジュール（依存リポジトリからコピー）
```

# 前提条件
1. アプリケーションのビルド環境が存在していることが必要です。
    | ソフトウェア名 | ディストリビューション | バージョン |
    | -------------- | ---------------------- | ---------- |
    | Python         | -                      | 3.13.9     |

2. AWS Secrets Managerを準備していることが必要です。

3. 依存リポジトリの使用準備が完了していることが必要です。
    - 依存リポジトリ
        | リポジトリ名 | 備考                             |
        | ------------ | -------------------------------- |
        | repo_infradx_ap_BSC_0020         | 共通ライブラリとして使用します。 |
        
        ※詳細は[リポジトリ一覧](#リポジトリ一覧)を参照

4. PostgreSQL、PosgGISの使用準備が完了していることが必要です。
    - DDL実行については[repo_infradx_oss_DDL](https://github.com/infra-faci-pf/repo_infradx_oss_DDL)を参照してください。

# リポジトリ利用方法
必要なソフトウェアやサービスの詳細は[前提条件](#前提条件)をご参照ください。
1. 依存リポジトリ（repo_infradx_ap_BSC_0020）から共通モジュールをコピーします。
    ```bash
    # common、config、core、utilディレクトリのファイルをコピー
    cp -r <依存リポジトリのパス>/App/common/* App/common/
    cp -r <依存リポジトリのパス>/App/config/* App/config/
    cp -r <依存リポジトリのパス>/App/core/* App/core/
    cp -r <依存リポジトリのパス>/App/util/* App/util/
    ```

2. 依存ライブラリをインストールします。
    ```bash
    pip install -r App/requirements.txt
    ```

3. ソース記載の一部情報を変更します。
    下記はAWS Secrets Managerに関する情報となっております。
    リポジトリ上ではシークレット名はマスクされているため、適切な値に書き換えてください。
    | 記載ファイル | 書き換え対象                             | 概要                                                                              |
    | ------------ | ---------------------------------------- | --------------------------------------------------------------------------------- |
    | config.ini   | cloud.aws.secretmanager.secretname       | シークレット名指定                                                                |

4. AWS Secrets Managerに以下の設定値を登録します。
    | キー名                 | 概要                                   |
    | ---------------------- | -------------------------------------- |
    | db_mst_schema          | マスタDBスキーマ名                     |
    | db_fac_schema          | 設備データDBスキーマ名                 |
    | db_work_schema         | 一時DBスキーマ名                       |
    | numeric_columns        | 数値項目として扱うカラム名一覧（カンマ区切り） |
    | date_columns           | 日付項目として扱うカラム名一覧（カンマ区切り） |

5. Pythonスクリプトを実行します。
    ```bash
    cd App/functions
    python FDI_0080_tblExportEquipmentWorkToMaster.py --import_id=<取込ID>
    ```
    ※取込IDは必須パラメータです（複数指定可能、カンマ区切り）

6. 実行ログを確認します。
    処理実行後、以下のようなログが出力されます。
    ```
    [2026-02-03 18:54:58,621] [INFO] [FDI] BPI0001:FDI_0080_TBL出力（一時DB→設備データ管理マスタDB）処理 開始
    [2026-02-03 18:54:58,710] [INFO] [FDI] BPI0002:FDI_0080_TBL出力（一時DB→設備データ管理マスタDB）処理 終了
    ```
    
    ログファイルの出力先は設定ファイル（`App/config/config.ini`）の以下の設定で定義されています。
    ```ini
    ; ログファイルパス
    log_file_path = /infradx/logs/infradx-batch/infradx-batch.log
    ```
    
    **注意:** ログファイルの出力先ディレクトリが存在しない場合、ログ出力エラーが発生します。実行前に以下のいずれかの対応を行ってください。
    - ログ出力ディレクトリを事前に作成する
      ```bash
      mkdir -p /infradx/logs/infradx-batch
      ```
    - または、`App/config/config.ini` の `log_file_path` を環境に合わせて変更する

# 利用OSS一覧
アプリケーションを利用するにあたり、以下OSSが必要です。
Python関連パッケージは App/requirements.txt に記載しており、`pip install -r App/requirements.txt` でインストールされます。

| OSS名       | バージョン | ライセンス                                   |
| ----------- | ---------- | -------------------------------------------- |
| Python      | 3.13.9     | PYTHON SOFTWARE FOUNDATION LICENSE VERSION 2 |
| PostgreSQL  | 16.8       | The PostgreSQL Licence                       |
| PostGIS     | 3.4        | GNU General Public License version 2         |
| boto3       | 1.40.74    | Apache License 2.0                           |
| psycopg2-binary | 2.9.10 | GNU Lesser General Public License v3         |
| concurrent-log-handler | 0.9.28 | Apache License 2.0              |

# 再配布OSS一覧
本リポジトリには再配布するOSSは含まれていません。
実行に必要なOSSは[利用OSS一覧](#利用OSS一覧)をご参照ください。

# 問い合わせに関して
1. 本リポジトリは配布を目的としており、IssueやPull Requestを受け付けておりません。

# ライセンス
1. 本リポジトリはMIT Licenseで提供されています。
2. ソースコードおよび関連ドキュメントの著作権はNTTインフラネット株式会社及び株式会社NTTデータに帰属します。

# 免責事項
1. 本リポジトリの内容は予告なく変更・削除する可能性があります。
2. 本リポジトリの利用により生じた損失及び損害等について、いかなる責任も負わないものとします。
