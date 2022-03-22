import getopt
import sys
import psycopg2
import toml
import time


def show_help():
    print("\nusage .eg: query_cli.py -c [tomlfile].toml")
    print("Toml file example:\n")
    print('db_connection="host=x1 dbname=x2 user=x3 password=x4')
    print('sql=""" multiline """')
    exit()


def load_config():
    config_dict = {}
    try:
        argv = sys.argv[1:]
        if len(argv) == 0:
            print("usage .eg: query_cli.py -c [tomlfile].toml")
            raise Exception("Out of arguments exception.")
        opts, _ = getopt.getopt(
            argv,
            "hc:",
            [
                "help",
                "config=",
            ],
        )
    except getopt.GetoptError as err:
        print(err)
        sys.exit(2)

    for opt, arg in opts:
        if opt == "-h":
            show_help()
        if opt in ("--config", "-c"):
            with open(f"./{arg}", "r") as f:
                conf = f.read()
                config_dict = toml.loads(conf)
        elif opt in ("--help"):
            show_help()
    return config_dict


if __name__ == "__main__":
    try:
        config_dict = load_config()
        rows = None
        try:
            start = time.monotonic()
            conn = psycopg2.connect(config_dict["db_connection"])
            conn.set_session(readonly=True, autocommit=True)
            cursor = conn.cursor()
            cursor.execute(config_dict["sql"])
            rows = cursor.fetchall()
        except (psycopg2.Error) as dbex:
            print(f"DB ERROR: {dbex}")
        finally:
            if not cursor.closed:
                cursor.close()
            end = time.monotonic()
            if rows:
                print(rows)
            print("elapsed time: {:.2f}s".format(end - start))

    except Exception as ex:
        print(ex)
