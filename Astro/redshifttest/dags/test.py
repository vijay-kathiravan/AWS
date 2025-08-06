import os
from flask import Flask, Response
import pandas as pd
def hosting_csv_data():
    """
    This function is here to host the given CSV file into a API based streaming which will
    be fed to Kinesis data stream
"""
    app = Flask("__main__")

    @app.route('/spotify')
    def get_csv():
        df = pd.read_csv("dataset.csv")
        return Response(df.to_csv(index=False), mimetype='text/csv')

    if __name__ == '__main__':
        app.run(host='0.0.0.0', port=8888)
hosting_csv_data()