from pickle import load, dump
import pandas as pd
import sys

def run(folder):
    clf = load(open(f'{folder}/model.pkl', 'rb'))
    X_train = pd.read_csv('X_train.csv', index_col=0)
    y_train = pd.read_csv('y_train.csv', index_col=0)
    print(clf)
    clf.fit(X_train, y_train)
    dump(clf, open(f'{folder}/fitted_model.pkl', 'wb'))


#run('Bayes')
run('XGB')
