# Titanic Datasets Metadata Summary

This document summarises the metadata for three CSV files used in the Titanic data analysis: titanic3.csv (source dataset), train.csv, and test.csv (Kaggle competition derivatives).

## Overview

The datasets describe passengers on the RMS Titanic (excluding crew), with details on survival, demographics, and travel information. `titanic3.csv` is the comprehensive source, split into `train.csv` (training) and `test.csv` (testing) for the Kaggle Titanic competition.

### 1. `titanic3.csv`

Source: Philip Hind’s Encyclopedia Titanica (1999), based on Michael A. Findlay’s Titanic Passenger List (Eaton & Haas, 1994), per titanic3info.txt.
Rows: 1309 passengers.
Columns: 14.
Description: Comprehensive dataset covering all Titanic passengers (no crew), with actual or estimated ages for ~80% of passengers. Source for train.csv and test.csv.
Columns:
pclass: Integer, passenger class (1 = 1st, 2 = 2nd, 3 = 3rd; proxy for socio-economic status: 1st = Upper, 2nd = Middle, 3rd = Lower).
survived: Integer, 0 (No) or 1 (Yes).
name: String, passenger name (e.g., "Allen, Miss. Elisabeth Walton").
sex: String, male or female.
age: Float, age in years (fractional for <1, xx.5 for estimated).
sibsp: Integer, number of siblings/spouses aboard (siblings: brother, sister, stepbrother, stepsister; spouses: husband, wife; excludes mistresses/fiancées).
parch: Integer, number of parents/children aboard (parents: mother, father; children: son, daughter, stepson, stepdaughter).
ticket: String, ticket number.
fare: Float, passenger fare in pre-1970 British pounds (1£ = 12s = 240d).
cabin: String, cabin number (many missing).
embarked: String, port of embarkation (C = Cherbourg, Q = Queenstown, S = Southampton).
boat: String, lifeboat number (if survived; many missing).
body: String, body identification number (if recovered; many missing).
home.dest: String, home/destination city (many missing).


James Kelly Entries:
PassengerId 697: "Kelly, Mr. James", male, age=44, pclass=3, embarked=S, ticket=363592.


Notes (from titanic3info.txt):
Excludes crew and non-family relations (e.g., cousins, aunts/uncles).
Some children traveled with nannies (parch=0) or friends (not counted in sibsp/parch).
Low sibsp (mean ~0.34 for age >25) and parch (mean ~1.37 for age <14) due to 3rd-class passengers traveling alone or with non-family.
Ideal for teaching logistic regression and imputation.



2. train.csv

Source: Kaggle Titanic competition, derived from titanic3.csv by dropping boat, body, home.dest and selecting 891 rows with survived.
Rows: 891 passengers.
Columns: 12.
Description: Training data for machine learning models, with survival outcomes.
Columns: Same as titanic3.csv (capitalised: Pclass, Survived, Name, etc.), excluding boat, body, home.dest.
James Kelly Entries: None.

3. test.csv

Source: Kaggle Titanic competition, derived from titanic3.csv by dropping boat, body, home.dest and selecting 418 rows without survived.
Rows: 418 passengers.
Columns: 11 (excludes Survived).
Description: Test data for predicting survival outcomes.
Columns: Same as train.csv, excluding Survived.
James Kelly Entries:
PassengerId 892: "Kelly, Mr. James", male, Age=34.5, Pclass=3, Embarked=Q, Ticket=330911.



Reconciliation

Row Count: train.csv (891) + test.csv (418) = 1309, matching titanic3.csv (1309).
Columns: train.csv and test.csv are subsets of titanic3.csv, dropping boat, body, home.dest. Column names are capitalised.
James Kelly Issue:
titanic3.csv: One Kelly (PassengerId 697, age 44, Southampton, ticket 363592).
test.csv: One Kelly (PassengerId 892, age 34.5, Queenstown, ticket 330911).
Encyclopedia Titanica: Two distinct James Kellys (19, Southampton; 44, Queenstown).
Discrepancy: titanic3.csv and Kaggle datasets conflate the two Kellys into single PassengerId entries with incorrect ages (44, 34.5), potentially discarding one Kelly.


Verification: Shared columns match exactly (accounting for case), per prior checks.

Notes

The datasets are ideal for logistic regression and imputation, per titanic3info.txt.
The James Kelly conflation suggests errors in titanic3.csv, to be resolved with Encyclopedia Titanica data.
Use a composite key (name + age + embarked + ticket) to retain both Kellys for analysis.
