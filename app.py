from flask import request, jsonify, Flask
from doltpy.core import Dolt, read as dolt_read, write as dolt_write
import argparse
from typing import Optional, List
import pandas as pd

app = Flask(__name__)
DOLT: Optional[Dolt] = None


class DoltCheckoutContext:
    def __init__(self, dolt: Dolt, checkout_branch: str, create: bool):
        self.dolt = dolt
        self.checkout_branch = checkout_branch
        self.create = create
        self.current_branch, self.branches = DOLT.branch()

    def __enter__(self):
        if self.checkout_branch != self.current_branch.name:
            if self.create:
                DOLT.checkout(self.checkout_branch,
                              checkout_branch=self.checkout_branch not in [branch.name for branch in self.branches])
            else:
                DOLT.checkout(self.current_branch.name)
        else:
            if not self.create:
                # sort out what to do with this
                raise

    def __exit__(self):
        if self.checkout_branch != self.current_branch.name:
            DOLT.checkout(self.current_branch.name)


def _extract_parameter(payload: dict, parameter: str, parameter_type: type):
    if parameter in payload:
        parameter_value = payload[parameter]
        if type(parameter_value) != parameter_type:
            return parameter_value, ValueError('{} is not of type {}'.format(parameter_value, parameter_type))
        return parameter_value, None
    else:
        return None, ValueError('{} missing from {}'.format(parameter, payload))


def _import_and_commit(dolt: Dolt, table: str, data: pd.DataFrame, primary_keys: Optional[List[str]], import_mode: str):
    dolt_write.import_df(dolt, table, pd.DataFrame(data), primary_keys, import_mode)
    dolt.add(table)
    dolt.commit('Executed import on table {} in import mode "{}"'.format(table, import_mode))
    commit = dolt.log()[0]

    return {
        'commit_hash': commit.hash,
        'timestamp': commit.ts,
        'author': commit.author,
        'message': commit.message
    }


@app.route('/api/update_table', methods=['POST'])
def update_table():
    payload = request.get_json()

    branch, branch_err = _extract_parameter(payload, 'branch', str)
    table, table_err = _extract_parameter(payload, 'table', str)
    data, data_err = _extract_parameter(payload, 'data', str)

    # validate the inputs

    with DoltCheckoutContext(DOLT, branch, True):
        tables = DOLT.ls()
        if table not in [table.name for table in tables]:
            # table must exist
            raise
        else:
            commit_details = _import_and_commit(DOLT, table, pd.DataFrame(data), None, dolt_write.UPDATE)
            return jsonify(commit_details)


@app.route('/api/create_table', methods=['POST'])
def create_table():
    payload = request.get_json()

    branch, branch_err = _extract_parameter(payload, 'branch', str)
    table, table_err = _extract_parameter(payload, 'table', str)
    primary_keys, primary_keys_err = _extract_parameter(payload, 'primary_keys', str)
    data, data_err = _extract_parameter(payload, 'data', str)

    # validate the inputs

    with DoltCheckoutContext(DOLT, branch, True):
        tables = DOLT.ls()
        if table in [table.name for table in tables]:
            # table must not exist
            raise
        else:
            commit_details = _import_and_commit(DOLT, table, pd.DataFrame(data), primary_keys, dolt_write.CREATE)
            return jsonify(commit_details)


@app.route('/api/read_table', methods=['GET'])
def read():
    payload = request.get_json()

    table, table_error = _extract_parameter(payload, 'table', str)
    branch, branch_error = _extract_parameter(payload, 'branch', str)

    # validate that we don't have errors

    with DoltCheckoutContext(DOLT, branch, False):
        data = dolt_read.read_table(DOLT, table).to_dict('rows')
        return jsonify(data)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dolt-path', help='The location of the Dolt database on the server machine')
    args = parser.parse_args()
    global DOLT
    DOLT = Dolt(args.dolt_path)
    app.run()


if __name__ == '__main__':
    main()
