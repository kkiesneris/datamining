import pandas as pd
import pickle
import seaborn as sns
import matplotlib.pyplot as plt
from sqlalchemy.engine import create_engine

from sklearn.decomposition import PCA
from sklearn.manifold import TSNE
from sklearn.preprocessing import StandardScaler
from umap import UMAP
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, roc_curve, precision_recall_curve, auc, f1_score, accuracy_score
from sklearn.svm import SVC
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier, plot_tree
from sklearn.naive_bayes import GaussianNB
from imblearn.over_sampling import SMOTE
from sklearn.model_selection import train_test_split


label_column = 'fraud'
id_column = 'transaction_id'
category_columns = ['country_code','mcc','merchant_id','pan','terminal_id','tran_date','transaction_id']
all_attributes = ['able_to_enter_pin','card_not_present','high_risk_mcc','neighbouring_country','pan_from_chip','pin_present',
                'previous_tran_declined','same_country','same_merchant','sepa_country','tran_at_night','tran_declined',
                'all_count_01d','all_count_07d','all_count_10m','all_count_30d','all_count_60d','all_count_60m',
                'all_day_avg_01d','all_day_avg_07d','all_day_avg_30d','all_day_avg_60d',
                'all_sum_01d','all_sum_07d','all_sum_30d','all_sum_60d',
                'all_tran_avg_01d','all_tran_avg_07d','all_tran_avg_30d','all_tran_avg_60d',
                'amount','amount_diff',#'pin_tries',
                'cnp_count_01d','cnp_count_07d','cnp_count_10m','cnp_count_30d','cnp_count_60d','cnp_count_60m',
                'cnp_day_avg_01d','cnp_day_avg_07d','cnp_day_avg_30d','cnp_day_avg_60d',
                'cnp_sum_01d','cnp_sum_07d','cnp_sum_30d','cnp_sum_60d',
                'cnp_tran_avg_01d','cnp_tran_avg_07d','cnp_tran_avg_30d','cnp_tran_avg_60d',
                'country_count_01d','country_count_07d','country_count_10m','country_count_30d','country_count_60d','country_count_60m',
                'country_day_avg_01d','country_day_avg_07d','country_day_avg_30d','country_day_avg_60d',
                'country_sum_01d','country_sum_07d','country_sum_30d','country_sum_60d',
                'country_tran_avg_01d','country_tran_avg_07d','country_tran_avg_30d','country_tran_avg_60d',
                'declined_count_01d','declined_count_07d','declined_count_10m','declined_count_30d','declined_count_60d','declined_count_60m',
                'first_cnp_tran','first_country_tran','first_declined_tran','first_mcc_tran','first_merchant_tran','first_nochip_tran','first_tran',
                'mcc_count_01d','mcc_count_07d','mcc_count_10m','mcc_count_30d','mcc_count_60d','mcc_count_60m',
                'mcc_day_avg_01d','mcc_day_avg_07d','mcc_day_avg_30d','mcc_day_avg_60d',
                'mcc_sum_01d','mcc_sum_07d','mcc_sum_30d','mcc_sum_60d',
                'mcc_tran_avg_01d','mcc_tran_avg_07d','mcc_tran_avg_30d','mcc_tran_avg_60d',
                'merchant_count_01d','merchant_count_07d','merchant_count_10m','merchant_count_30d','merchant_count_60d','merchant_count_60m',
                'merchant_day_avg_01d','merchant_day_avg_07d','merchant_day_avg_30d','merchant_day_avg_60d',
                'merchant_sum_01d','merchant_sum_07d','merchant_sum_30d','merchant_sum_60d',
                'merchant_tran_avg_01d','merchant_tran_avg_07d','merchant_tran_avg_30d','merchant_tran_avg_60d',
                'nochip_count_01d','nochip_count_07d','nochip_count_10m','nochip_count_30d','nochip_count_60d','nochip_count_60m',
                'nochip_day_avg_01d','nochip_day_avg_07d','nochip_day_avg_30d','nochip_day_avg_60d',
                'nochip_sum_01d','nochip_sum_07d','nochip_sum_30d','nochip_sum_60d',
                'nochip_tran_avg_01d','nochip_tran_avg_07d','nochip_tran_avg_30d','nochip_tran_avg_60d',
                'time_since_prev_country_tran','time_since_prev_mcc_tran','time_since_prev_merchant_tran','time_since_prev_tran']

#Attributes by groups
attributes=[
    {
        'label':'Skaitliski atribūti',
        'name':'numerical',
        'type':'numerical',
        'value':['amount','amount_diff']#,'pin_tries']
    },
    {
        'label':'Bināri atribūti',
        'name':'binary',
        'type':'binary',
        'value':['able_to_enter_pin','card_not_present',
                    'first_cnp_tran','first_country_tran','first_declined_tran','first_mcc_tran','first_merchant_tran','first_nochip_tran','first_tran',
                    'high_risk_mcc','neighbouring_country','pan_from_chip','pin_present','previous_tran_declined',
                    'same_country','same_merchant','sepa_country','tran_at_night','tran_declined']
    },
    {
        'label':'Atteikto transakciju skaits noteiktā laika intervālā',
        'name':'declined',
        'type':'aggregated',
        'value':['declined_count_01d','declined_count_07d','declined_count_10m','declined_count_30d','declined_count_60d','declined_count_60m']
    },
    {
        'label':'Laiks kopš iepriekšējās transakcijas dažādās kategorijās',
        'name':'since',
        'type':'aggregated',
        'value':['time_since_prev_country_tran','time_since_prev_mcc_tran','time_since_prev_merchant_tran','time_since_prev_tran']
    },
    {
        'label':'Transakciju skaits, summa, vidējā summa noteiktā laika periodā',
        'name':'nogroup',
        'type':'aggregated',
        'value':['all_count_01d','all_count_07d','all_count_10m','all_count_30d','all_count_60d','all_count_60m',
                    'all_day_avg_01d','all_day_avg_07d','all_day_avg_30d','all_day_avg_60d',
                    'all_sum_01d','all_sum_07d','all_sum_30d','all_sum_60d',
                    'all_tran_avg_01d','all_tran_avg_07d','all_tran_avg_30d','all_tran_avg_60d']
    },
    {
        'label':'Transakciju skaits stundā un 30 dienās',
        'name':'nogroupCount60m30d',
        'type':'aggregated',
        'value':['all_count_30d','all_count_60m']
    },
    {
        'label':'Transakciju summa 1 dienā un 30 dienās',
        'name':'nogroupSum01d30d',
        'type':'aggregated',
        'value':['all_sum_01d','all_sum_30d']
    },
    {
        'label':'Vidējā transakcijas summa 1 dienā un 30 dienās',
        'name':'nogroupTranAvg01d30d',
        'type':'aggregated',
        'value':['all_tran_avg_01d','all_tran_avg_30d']
    },
    {
        'label':'Vidējā summa 1 dienā un 30 dienās',
        'name':'nogroupDayAvg01d30d',
        'type':'aggregated',
        'value':['all_day_avg_01d','all_day_avg_30d']
    },
    {
        'label':'Transakciju skaits, summa, vidējā summa kartei neesot klāt noteiktā laika periodā',
        'name':'cnp',
        'type':'aggregated',
        'value':['cnp_count_01d','cnp_count_07d','cnp_count_10m','cnp_count_30d','cnp_count_60d','cnp_count_60m',
                    'cnp_day_avg_01d','cnp_day_avg_07d','cnp_day_avg_30d','cnp_day_avg_60d',
                    'cnp_sum_01d','cnp_sum_07d','cnp_sum_30d','cnp_sum_60d',
                    'cnp_tran_avg_01d','cnp_tran_avg_07d','cnp_tran_avg_30d','cnp_tran_avg_60d']
    },
    {
        'label':'Transakciju skaits stundā un 30 dienās kartei neesot klāt',
        'name':'cnpCount60m30d',
        'type':'aggregated',
        'value':['cnp_count_30d','cnp_count_60m']
    },
    {
        'label':'Transakciju summa 1 dienā un 30 dienās kartei neesot klāt',
        'name':'cnpSum01d30d',
        'type':'aggregated',
        'value':['cnp_sum_01d','cnp_sum_30d']
    },
    {
        'label':'Vidējā transakcijas summa 1 dienā un 30 dienās kartei neesot klāt',
        'name':'cnpTranAvg01d30d',
        'type':'aggregated',
        'value':['cnp_tran_avg_01d','cnp_tran_avg_30d']
    },
    {
        'label':'Vidējā summa 1 dienā un 30 dienās kartei neesot klāt',
        'name':'cnpDayAvg01d30d',
        'type':'aggregated',
        'value':['cnp_day_avg_01d','cnp_day_avg_30d']
    },
    {
        'label':'Transakciju skaits, summa, vidējā summa valstī noteiktā laika periodā',
        'name':'country',
        'type':'aggregated',
        'value':['country_count_01d','country_count_07d','country_count_10m','country_count_30d','country_count_60d','country_count_60m',
                    'country_day_avg_01d','country_day_avg_07d','country_day_avg_30d','country_day_avg_60d',
                    'country_sum_01d','country_sum_07d','country_sum_30d','country_sum_60d',
                    'country_tran_avg_01d','country_tran_avg_07d','country_tran_avg_30d','country_tran_avg_60d']
    },
    {
        'label':'Transakciju skaits stundā un 30 dienās valstī',
        'name':'countryCount60m30d',
        'type':'aggregated',
        'value':['country_count_30d','country_count_60m']
    },
    {
        'label':'Transakciju summa 1 dienā un 30 dienās valstī',
        'name':'countrySum01d30d',
        'type':'aggregated',
        'value':['country_sum_01d','country_sum_30d']
    },
    {
        'label':'Vidējā transakcijas summa 1 dienā un 30 dienās valstī',
        'name':'countryTranAvg01d30d',
        'type':'aggregated',
        'value':['country_tran_avg_01d','country_tran_avg_30d']
    },
    {
        'label':'Vidējā summa 1 dienā un 30 dienās valstī',
        'name':'countryDayAvg01d30d',
        'type':'aggregated',
        'value':['country_day_avg_01d','country_day_avg_30d']
    },
    {
        'label':'Transakciju skaits, summa, vidējā summa pie tirgotāju kategorijas noteiktā laika periodā',
        'name':'mcc',
        'type':'aggregated',
        'value':['mcc_count_01d','mcc_count_07d','mcc_count_10m','mcc_count_30d','mcc_count_60d','mcc_count_60m',
                    'mcc_day_avg_01d','mcc_day_avg_07d','mcc_day_avg_30d','mcc_day_avg_60d',
                    'mcc_sum_01d','mcc_sum_07d','mcc_sum_30d','mcc_sum_60d',
                    'mcc_tran_avg_01d','mcc_tran_avg_07d','mcc_tran_avg_30d','mcc_tran_avg_60d']
    },
    {
        'label':'Transakciju skaits stundā un 30 dienās pie tirgotāju kategorijas',
        'name':'mccCount60m30d',
        'type':'aggregated',
        'value':['mcc_count_30d','mcc_count_60m']
    },
    {
        'label':'Transakciju summa 1 dienā un 30 dienās pie tirgotāju kategorijas',
        'name':'mccSum01d30d',
        'type':'aggregated',
        'value':['mcc_sum_01d','mcc_sum_30d']
    },
    {
        'label':'Vidējā transakcijas summa 1 dienā un 30 dienās pie tirgotāju kategorijas',
        'name':'mccTranAvg01d30d',
        'type':'aggregated',
        'value':['mcc_tran_avg_01d','mcc_tran_avg_30d']
    },
    {
        'label':'Vidējā summa 1 dienā un 30 dienās pie tirgotāju kategorijas',
        'name':'mccDayAvg01d30d',
        'type':'aggregated',
        'value':['mcc_day_avg_01d','mcc_day_avg_30d']
    },
    {
        'label':'Transakciju skaits, summa, vidējā summa pie tirgotāja noteiktā laika periodā',
        'name':'merchant',
        'type':'aggregated',
        'value':['merchant_count_01d','merchant_count_07d','merchant_count_10m','merchant_count_30d','merchant_count_60d','merchant_count_60m'
                    'merchant_day_avg_01d','merchant_day_avg_07d','merchant_day_avg_30d','merchant_day_avg_60d',
                    'merchant_sum_01d','merchant_sum_07d','merchant_sum_30d','merchant_sum_60d',
                    'merchant_tran_avg_01d','merchant_tran_avg_07d','merchant_tran_avg_30d','merchant_tran_avg_60d']
    },
    {
        'label':'Transakciju skaits stundā un 30 dienās pie tirgotāja',
        'name':'merchantCount60m30d',
        'type':'aggregated',
        'value':['merchant_count_30d','merchant_count_60m']
    },
    {
        'label':'Transakciju summa 1 dienā un 30 dienās pie tirgotāja',
        'name':'merchantSum01d30d',
        'type':'aggregated',
        'value':['merchant_sum_01d','merchant_sum_30d']
    },
    {
        'label':'Vidējā transakcijas summa 1 dienā un 30 dienās pie tirgotāja',
        'name':'merchantTranAvg01d30d',
        'type':'aggregated',
        'value':['merchant_tran_avg_01d','merchant_tran_avg_30d']
    },
    {
        'label':'Vidējā summa 1 dienā un 30 dienās pie tirgotāja',
        'name':'merchantDayAvg01d30d',
        'type':'aggregated',
        'value':['merchant_day_avg_01d','merchant_day_avg_30d']
    },
    {
        'label':'Transakciju skaits, summa, vidējā summa nenolasot viedkartes mikroshēmu noteiktā laika periodā',
        'name':'nochip',
        'type':'aggregated',
        'value':['nochip_count_01d','nochip_count_07d','nochip_count_10m','nochip_count_30d','nochip_count_60d','nochip_count_60m',
                    'nochip_day_avg_01d','nochip_day_avg_07d','nochip_day_avg_30d','nochip_day_avg_60d',
                    'nochip_sum_01d','nochip_sum_07d','nochip_sum_30d','nochip_sum_60d',
                    'nochip_tran_avg_01d','nochip_tran_avg_07d','nochip_tran_avg_30d','nochip_tran_avg_60d']
    },
    {
        'label':'Transakciju skaits stundā un 30 dienās nenolasot viedkartes mikroshēmu',
        'name':'nochipCount60m30d',
        'type':'aggregated',
        'value':['nochip_count_30d','nochip_count_60m']
    },
    {
        'label':'Transakciju summa 1 dienā un 30 dienās nenolasot viedkartes mikroshēmu',
        'name':'nochipSum01d30d',
        'type':'aggregated',
        'value':['nochip_sum_01d','nochip_sum_30d']
    },
    {
        'label':'Vidējā transakcijas summa 1 dienā un 30 dienās nenolasot viedkartes mikroshēmu',
        'name':'nochipTranAvg01d30d',
        'type':'aggregated',
        'value':['nochip_tran_avg_01d','nochip_tran_avg_30d']
    },
    {
        'label':'Vidējā summa 1 dienā un 30 dienās nenolasot viedkartes mikroshēmu',
        'name':'nochipDayAvg01d30d',
        'type':'aggregated',
        'value':['nochip_day_avg_01d','nochip_day_avg_30d']
    },
    {
        'label':'Transakciju skaits 10 minūšu intervālā dažādās kategorijās',
        'name':'10m',
        'type':'aggregated',
        'value':['all_count_10m','cnp_count_10m','country_count_10m','declined_count_10m','mcc_count_10m','merchant_count_10m','nochip_count_10m']
    },
    {
        'label':'Transakciju skaits 60 minūšu intervālā dažādās kategorijās',
        'name':'60m',
        'type':'aggregated',
        'value':['all_count_60m','cnp_count_60m','country_count_60m','declined_count_60m','mcc_count_60m','merchant_count_60m','nochip_count_60m']
    },
    {
        'label':'Transakciju skaits 1 dienas intervālā dažādās kategorijās',
        'name':'01d',
        'type':'aggregated',
        'value':['all_count_01d','all_day_avg_01d','all_sum_01d','all_tran_avg_01d',
                 'cnp_count_01d','cnp_day_avg_01d','cnp_sum_01d','cnp_tran_avg_01d',
                 'country_count_01d','country_day_avg_01d','country_sum_01d','country_tran_avg_01d',
                 'mcc_count_01d','mcc_day_avg_01d','mcc_sum_01d','mcc_tran_avg_01d',
                 'merchant_count_01d','merchant_day_avg_01d','merchant_sum_01d','merchant_tran_avg_01d',
                 'nochip_count_01d','nochip_day_avg_01d','nochip_sum_01d','nochip_tran_avg_01d']
    },
    {
        'label':'Transakciju skaits 7 dienu intervālā dažādās kategorijās',
        'name':'07d',
        'type':'aggregated',
        'value':['all_count_07d','all_day_avg_07d','all_sum_07d','all_tran_avg_07d',
                 'cnp_count_07d','cnp_day_avg_07d','cnp_sum_07d','cnp_tran_avg_07d',
                 'country_count_07d','country_day_avg_07d','country_sum_07d','country_tran_avg_07d',
                 'declined_count_07d',
                 'mcc_count_07d','mcc_day_avg_07d','mcc_sum_07d','mcc_tran_avg_07d',
                 'merchant_count_07d','merchant_day_avg_07d','merchant_sum_07d','merchant_tran_avg_07d',
                 'nochip_count_07d','nochip_day_avg_07d','nochip_sum_07d','nochip_tran_avg_07d']
    },
    {
        'label':'Transakciju skaits 30 dienu intervālā dažādās kategorijās',
        'name':'30d',
        'type':'aggregated',
        'value':['all_count_30d','all_day_avg_30d','all_sum_30d','all_tran_avg_30d',
                 'cnp_count_30d','cnp_day_avg_30d','cnp_sum_30d','cnp_tran_avg_30d',
                 'country_count_30d','country_day_avg_30d','country_sum_30d','country_tran_avg_30d',
                 'declined_count_30d',
                 'mcc_count_30d','mcc_day_avg_30d','mcc_sum_30d','mcc_tran_avg_30d',
                 'merchant_count_30d','merchant_day_avg_30d','merchant_sum_30d','merchant_tran_avg_30d',
                 'nochip_count_30d','nochip_day_avg_30d','nochip_sum_30d','nochip_tran_avg_30d']
    },
    {
        'label':'Transakciju skaits 60 dienu intervālā dažādās kategorijās',
        'name':'60d',
        'type':'aggregated',
        'value':['all_count_60d','all_day_avg_60d','all_sum_60d','all_tran_avg_60d',
                 'cnp_count_60d','cnp_day_avg_60d','cnp_sum_60d','cnp_tran_avg_60d',
                 'country_count_60d','country_day_avg_60d','country_sum_60d','country_tran_avg_60d',
                 'declined_count_60d',
                 'mcc_count_60d','mcc_day_avg_60d','mcc_sum_60d','mcc_tran_avg_60d',
                 'merchant_count_60d','merchant_day_avg_60d','merchant_sum_60d','merchant_tran_avg_60d',
                 'nochip_count_60d','nochip_day_avg_60d','nochip_sum_60d','nochip_tran_avg_60d']
    },
    {
        'label':'Pirmreizējas transakcijas dažādās kategorijās',
        'name':'firsttime',
        'type':'aggregated',
        'value':['first_cnp_tran','first_country_tran','first_declined_tran','first_mcc_tran','first_merchant_tran','first_nochip_tran','first_tran']
    },
    {
        'label':'Visi skaitliskie transakciju parametri',
        'name':'all',
        'type':'all',
        'value':['able_to_enter_pin','card_not_present','high_risk_mcc','neighbouring_country','pan_from_chip','pin_present',
                'previous_tran_declined','same_country','same_merchant','sepa_country','tran_at_night','tran_declined',
                'all_count_01d','all_count_07d','all_count_10m','all_count_30d','all_count_60d','all_count_60m',
                'all_day_avg_01d','all_day_avg_07d','all_day_avg_30d','all_day_avg_60d',
                'all_sum_01d','all_sum_07d','all_sum_30d','all_sum_60d',
                'all_tran_avg_01d','all_tran_avg_07d','all_tran_avg_30d','all_tran_avg_60d',
                'amount','amount_diff',#'pin_tries',
                'cnp_count_01d','cnp_count_07d','cnp_count_10m','cnp_count_30d','cnp_count_60d','cnp_count_60m',
                'cnp_day_avg_01d','cnp_day_avg_07d','cnp_day_avg_30d','cnp_day_avg_60d',
                'cnp_sum_01d','cnp_sum_07d','cnp_sum_30d','cnp_sum_60d',
                'cnp_tran_avg_01d','cnp_tran_avg_07d','cnp_tran_avg_30d','cnp_tran_avg_60d',
                'country_count_01d','country_count_07d','country_count_10m','country_count_30d','country_count_60d','country_count_60m',
                'country_day_avg_01d','country_day_avg_07d','country_day_avg_30d','country_day_avg_60d',
                'country_sum_01d','country_sum_07d','country_sum_30d','country_sum_60d',
                'country_tran_avg_01d','country_tran_avg_07d','country_tran_avg_30d','country_tran_avg_60d',
                'declined_count_01d','declined_count_07d','declined_count_10m','declined_count_30d','declined_count_60d','declined_count_60m',
                'first_cnp_tran','first_country_tran','first_declined_tran','first_mcc_tran','first_merchant_tran','first_nochip_tran','first_tran',
                'mcc_count_01d','mcc_count_07d','mcc_count_10m','mcc_count_30d','mcc_count_60d','mcc_count_60m',
                'mcc_day_avg_01d','mcc_day_avg_07d','mcc_day_avg_30d','mcc_day_avg_60d',
                'mcc_sum_01d','mcc_sum_07d','mcc_sum_30d','mcc_sum_60d',
                'mcc_tran_avg_01d','mcc_tran_avg_07d','mcc_tran_avg_30d','mcc_tran_avg_60d',
                'merchant_count_01d','merchant_count_07d','merchant_count_10m','merchant_count_30d','merchant_count_60d','merchant_count_60m',
                'merchant_day_avg_01d','merchant_day_avg_07d','merchant_day_avg_30d','merchant_day_avg_60d',
                'merchant_sum_01d','merchant_sum_07d','merchant_sum_30d','merchant_sum_60d',
                'merchant_tran_avg_01d','merchant_tran_avg_07d','merchant_tran_avg_30d','merchant_tran_avg_60d',
                'nochip_count_01d','nochip_count_07d','nochip_count_10m','nochip_count_30d','nochip_count_60d','nochip_count_60m',
                'nochip_day_avg_01d','nochip_day_avg_07d','nochip_day_avg_30d','nochip_day_avg_60d',
                'nochip_sum_01d','nochip_sum_07d','nochip_sum_30d','nochip_sum_60d',
                'nochip_tran_avg_01d','nochip_tran_avg_07d','nochip_tran_avg_30d','nochip_tran_avg_60d',
                'time_since_prev_country_tran','time_since_prev_mcc_tran','time_since_prev_merchant_tran','time_since_prev_tran']
    },
    {
        'label':'ReliefF 10 atlasītie atribūti datu kopai A',
        'name':'relieff10A',
        'type':'relieff',
        'value':['pan_from_chip', 'pin_present', 'tran_declined', 'able_to_enter_pin', 'first_mcc_tran', 'time_since_prev_mcc_tran', 'sepa_country', 'nochip_count_01d', 'previous_tran_declined', 'mcc_count_60m']
    },
    {
        'label':'ReliefF 10 atlasītie atribūti datu kopai B',
        'name':'relieff10B',
        'type':'relieff',
        'value':['pan_from_chip', 'able_to_enter_pin', 'pin_present', 'sepa_country', 'first_mcc_tran', 'time_since_prev_mcc_tran', 'tran_at_night', 'tran_declined', 'same_country', 'first_merchant_tran']
    },
    {
        'label':'ReliefF 10 atlasītie atribūti ar iztveršanas metodi sabalansētai datu kopai A',
        'name':'relieff10Au',
        'type':'relieff',
        'value':['pan_from_chip', 'pin_present', 'tran_declined', 'first_nochip_tran', 'first_tran', 'time_since_prev_tran', 'previous_tran_declined', 'nochip_count_01d', 'all_count_30d', 'able_to_enter_pin']
    },
    {
        'label':'ReliefF 10 atlasītie atribūti ar iztveršanas metodi sabalansētai datu kopai B',
        'name':'relieff10Bu',
        'type':'relieff',
        'value':['pan_from_chip', 'pin_present', 'able_to_enter_pin', 'tran_declined', 'sepa_country', 'nochip_count_30d', 'tran_at_night', 'same_merchant', 'nochip_tran_avg_60d', 'nochip_count_60d']
    },
    {
        'label':'ReliefF 10 atlasītie atribūti ar SMOTE metodi sabalansētai datu kopai A',
        'name':'relieff10Ao',
        'type':'relieff',
        'value':['pan_from_chip', 'pin_present', 'tran_declined', 'all_count_60d', 'all_count_30d', 'able_to_enter_pin', 'previous_tran_declined', 'nochip_count_30d', 'nochip_count_60d', 'amount']
    },
    {
        'label':'ReliefF 10 atlasītie atribūti ar SMOTE metodi sabalansētai datu kopai B',
        'name':'relieff10Bo',
        'type':'relieff',
        'value':['pan_from_chip', 'sepa_country', 'pin_present', 'tran_declined', 'able_to_enter_pin', 'nochip_count_60d', 'nochip_count_30d', 'all_count_60d', 'previous_tran_declined', 'all_count_30d']
    },
    {
        'label':'ReliefF visi 23 atlasītie atribūti',
        'name':'relieff23',
        'type':'relieff',
        'value':['first_tran', 'nochip_count_01d', 'pan_from_chip', 'sepa_country', 'tran_declined', 'mcc_count_60m', 'time_since_prev_mcc_tran', 'first_mcc_tran', 'pin_present', 'same_merchant',
                 'time_since_prev_tran', 'same_country', 'all_count_60d', 'all_count_30d', 'tran_at_night', 'nochip_count_30d', 'nochip_count_60d', 'previous_tran_declined', 'first_merchant_tran',
                 'amount', 'nochip_tran_avg_60d', 'able_to_enter_pin', 'first_nochip_tran']
    },
    {
        'label':'CART 10 vērtīgākie atribūti datu kopai A',
        'name':'cart10A',
        'type':'cart',
        'value':['tran_declined', 'amount', 'pin_present', 'country_day_avg_60d', 'declined_count_60d', 'all_count_30d', 'merchant_day_avg_07d', 'mcc_tran_avg_01d', 'amount_diff', 'able_to_enter_pin']
    },
    {
        'label':'CART 10 vērtīgākie atribūti datu kopai B',
        'name':'cart10B',
        'type':'cart',
        'value':['pan_from_chip', 'tran_declined', 'amount', 'mcc_sum_01d', 'high_risk_mcc', 'mcc_count_01d', 'mcc_day_avg_30d', 'declined_count_60d', 'country_count_60d', 'nochip_day_avg_30d']
    },
    {
        'label':'CART 10 vērtīgākie atribūti ar iztveršanas metodi sabalansētai datu kopai A',
        'name':'cart10Au',
        'type':'cart',
        'value':['pin_present', 'tran_declined', 'amount', 'all_sum_60d', 'amount_diff', 'country_count_60d', 'all_count_60d', 'time_since_prev_tran', 'all_day_avg_01d', 'declined_count_60d']
    },
    {
        'label':'CART 10 vērtīgākie atribūti ar iztveršanas metodi sabalansētai datu kopai B',
        'name':'cart10Bu',
        'type':'cart',
        'value':['pan_from_chip', 'tran_declined', 'nochip_count_60d', 'mcc_sum_01d', 'amount', 'nochip_sum_07d', 'all_sum_60d', 'country_count_30d', 'merchant_tran_avg_07d', 'merchant_sum_07d']
    },
    {
        'label':'CART 10 vērtīgākie atribūti ar SMOTE metodi sabalansētai datu kopai A',
        'name':'cart10Ao',
        'type':'cart',
        'value':['tran_declined', 'pan_from_chip', 'amount', 'nochip_sum_60d', 'amount_diff', 'all_count_60d', 'nochip_count_60d', 'nochip_day_avg_01d', 'declined_count_60d', 'all_day_avg_30d']
    },
    {
        'label':'CART 10 vērtīgākie atribūti ar SMOTE metodi sabalansētai datu kopai B',
        'name':'cart10Bo',
        'type':'cart',
        'value':['pan_from_chip', 'tran_declined', 'amount', 'mcc_sum_01d', 'all_count_60d', 'mcc_count_01d', 'cnp_count_01d', 'time_since_prev_tran', 'all_day_avg_07d', 'all_count_30d']
    },
    {
        'label':'CART visi 31 atlasītie atribūti',
        'name':'cart31',
        'type':'cart',
        'value':['pan_from_chip', 'tran_declined', 'nochip_day_avg_30d', 'country_day_avg_60d', 'cnp_count_01d', 'mcc_tran_avg_01d', 'nochip_sum_60d', 'amount_diff', 'high_risk_mcc',
                 'all_sum_60d', 'mcc_count_01d', 'merchant_sum_07d', 'amount', 'country_count_30d', 'mcc_sum_01d', 'pin_present', 'country_count_60d', 'mcc_day_avg_30d', 'all_count_30d',
                  'able_to_enter_pin', 'all_count_60d', 'time_since_prev_tran', 'all_day_avg_07d', 'nochip_day_avg_01d', 'declined_count_60d', 'merchant_tran_avg_07d', 'nochip_sum_07d',
                  'all_day_avg_01d', 'merchant_day_avg_07d', 'nochip_count_60d', 'all_day_avg_30d']
    },
    {
        'label':'CART pirmo trīs līmeņu atribūti no all atribūtu kopas datu kopai A',
        'name':'cartAllA',
        'type':'cart',
        'value':['pin_present','tran_declined','time_since_prev_country_tran','amount','cnp_tran_avg_07d','merchant_tran_avg_60d','all_count_60d']
    },
    {
        'label':'CART pirmo trīs līmeņu atribūti no all atribūtu kopas ar SMOTE sabalansētai datu kopai A',
        'name':'cartAllAo',
        'type':'cart',
        'value':['pan_from_chip','tran_declined','all_count_60d','amount','nochip_count_60d','mcc_day_avg_01d','same_merchant']
    },
    {
        'label':'CART pirmo trīs līmeņu atribūti no binary atribūtu kopas ar iztveršanu sabalansētai datu kopai A',
        'name':'cartBinaryAu',
        'type':'cart',
        'value':['tran_declined','pin_present','first_tran','pan_from_chip','able_to_enter_pin','card_not_present']
    },
    {
        'label':'CART pirmo trīs līmeņu atribūti no binary atribūtu kopas datu kopai B',
        'name':'cartBinaryB',
        'type':'cart',
        'value':['pan_from_chip','tran_declined','same_merchant','high_risk_mcc','first_tran','first_mcc_tran','pin_present']
    },
]


column_list = ['fraud','transaction_id',
        'able_to_enter_pin','card_not_present','high_risk_mcc','neighbouring_country','pan_from_chip','pin_present',
        'previous_tran_declined','same_country','same_merchant','sepa_country','tran_at_night','tran_declined',
        'all_count_01d','all_count_07d','all_count_10m','all_count_30d','all_count_60d','all_count_60m',
        'all_day_avg_01d','all_day_avg_07d','all_day_avg_30d','all_day_avg_60d',
        'all_sum_01d','all_sum_07d','all_sum_30d','all_sum_60d',
        'all_tran_avg_01d','all_tran_avg_07d','all_tran_avg_30d','all_tran_avg_60d',
        'amount','amount_diff',#'pin_tries',
        'cnp_count_01d','cnp_count_07d','cnp_count_10m','cnp_count_30d','cnp_count_60d','cnp_count_60m',
        'cnp_day_avg_01d','cnp_day_avg_07d','cnp_day_avg_30d','cnp_day_avg_60d',
        'cnp_sum_01d','cnp_sum_07d','cnp_sum_30d','cnp_sum_60d',
        'cnp_tran_avg_01d','cnp_tran_avg_07d','cnp_tran_avg_30d','cnp_tran_avg_60d',
        'country_count_01d','country_count_07d','country_count_10m','country_count_30d','country_count_60d','country_count_60m',
        'country_day_avg_01d','country_day_avg_07d','country_day_avg_30d','country_day_avg_60d',
        'country_sum_01d','country_sum_07d','country_sum_30d','country_sum_60d',
        'country_tran_avg_01d','country_tran_avg_07d','country_tran_avg_30d','country_tran_avg_60d',
        'declined_count_01d','declined_count_07d','declined_count_10m','declined_count_30d','declined_count_60d','declined_count_60m',
        'first_cnp_tran','first_country_tran','first_declined_tran','first_mcc_tran','first_merchant_tran','first_nochip_tran','first_tran',
        'mcc_count_01d','mcc_count_07d','mcc_count_10m','mcc_count_30d','mcc_count_60d','mcc_count_60m',
        'mcc_day_avg_01d','mcc_day_avg_07d','mcc_day_avg_30d','mcc_day_avg_60d',
        'mcc_sum_01d','mcc_sum_07d','mcc_sum_30d','mcc_sum_60d',
        'mcc_tran_avg_01d','mcc_tran_avg_07d','mcc_tran_avg_30d','mcc_tran_avg_60d',
        'merchant_count_01d','merchant_count_07d','merchant_count_10m','merchant_count_30d','merchant_count_60d','merchant_count_60m',
        'merchant_day_avg_01d','merchant_day_avg_07d','merchant_day_avg_30d','merchant_day_avg_60d',
        'merchant_sum_01d','merchant_sum_07d','merchant_sum_30d','merchant_sum_60d',
        'merchant_tran_avg_01d','merchant_tran_avg_07d','merchant_tran_avg_30d','merchant_tran_avg_60d',
        'nochip_count_01d','nochip_count_07d','nochip_count_10m','nochip_count_30d','nochip_count_60d','nochip_count_60m',
        'nochip_day_avg_01d','nochip_day_avg_07d','nochip_day_avg_30d','nochip_day_avg_60d',
        'nochip_sum_01d','nochip_sum_07d','nochip_sum_30d','nochip_sum_60d',
        'nochip_tran_avg_01d','nochip_tran_avg_07d','nochip_tran_avg_30d','nochip_tran_avg_60d',
        'time_since_prev_country_tran','time_since_prev_mcc_tran','time_since_prev_merchant_tran','time_since_prev_tran',
        'country_code','mcc','merchant_id'
    ]

column_str = """
        fraud,transaction_id,
        able_to_enter_pin,card_not_present,high_risk_mcc,neighbouring_country,pan_from_chip,pin_present,
        previous_tran_declined,same_country,same_merchant,sepa_country,tran_at_night,tran_declined,
        all_count_01d,all_count_07d,all_count_10m,all_count_30d,all_count_60d,all_count_60m,
        all_day_avg_01d,all_day_avg_07d,all_day_avg_30d,all_day_avg_60d,
        all_sum_01d,all_sum_07d,all_sum_30d,all_sum_60d,
        all_tran_avg_01d,all_tran_avg_07d,all_tran_avg_30d,all_tran_avg_60d,
        amount,amount_diff,
        cnp_count_01d,cnp_count_07d,cnp_count_10m,cnp_count_30d,cnp_count_60d,cnp_count_60m,
        cnp_day_avg_01d,cnp_day_avg_07d,cnp_day_avg_30d,cnp_day_avg_60d,
        cnp_sum_01d,cnp_sum_07d,cnp_sum_30d,cnp_sum_60d,
        cnp_tran_avg_01d,cnp_tran_avg_07d,cnp_tran_avg_30d,cnp_tran_avg_60d,
        country_count_01d,country_count_07d,country_count_10m,country_count_30d,country_count_60d,country_count_60m,
        country_day_avg_01d,country_day_avg_07d,country_day_avg_30d,country_day_avg_60d,
        country_sum_01d,country_sum_07d,country_sum_30d,country_sum_60d,
        country_tran_avg_01d,country_tran_avg_07d,country_tran_avg_30d,country_tran_avg_60d,
        declined_count_01d,declined_count_07d,declined_count_10m,declined_count_30d,declined_count_60d,declined_count_60m,
        first_cnp_tran,first_country_tran,first_declined_tran,first_mcc_tran,first_merchant_tran,first_nochip_tran,first_tran,
        mcc_count_01d,mcc_count_07d,mcc_count_10m,mcc_count_30d,mcc_count_60d,mcc_count_60m,
        mcc_day_avg_01d,mcc_day_avg_07d,mcc_day_avg_30d,mcc_day_avg_60d,
        mcc_sum_01d,mcc_sum_07d,mcc_sum_30d,mcc_sum_60d,
        mcc_tran_avg_01d,mcc_tran_avg_07d,mcc_tran_avg_30d,mcc_tran_avg_60d,
        merchant_count_01d,merchant_count_07d,merchant_count_10m,merchant_count_30d,merchant_count_60d,merchant_count_60m,
        merchant_day_avg_01d,merchant_day_avg_07d,merchant_day_avg_30d,merchant_day_avg_60d,
        merchant_sum_01d,merchant_sum_07d,merchant_sum_30d,merchant_sum_60d,
        merchant_tran_avg_01d,merchant_tran_avg_07d,merchant_tran_avg_30d,merchant_tran_avg_60d,
        nochip_count_01d,nochip_count_07d,nochip_count_10m,nochip_count_30d,nochip_count_60d,nochip_count_60m,
        nochip_day_avg_01d,nochip_day_avg_07d,nochip_day_avg_30d,nochip_day_avg_60d,
        nochip_sum_01d,nochip_sum_07d,nochip_sum_30d,nochip_sum_60d,
        nochip_tran_avg_01d,nochip_tran_avg_07d,nochip_tran_avg_30d,nochip_tran_avg_60d,
        time_since_prev_country_tran,time_since_prev_mcc_tran,time_since_prev_merchant_tran,time_since_prev_tran,
        country_code,mcc,merchant_id
        """
#pin_tries,
tranSet_A = "select "+column_str+"""
from transaction_vw t, pan_set p where t.pan = p.pan and p.set_id = 'A'
"""

tranSet_B = "select "+column_str+"""
from transaction_vw t, pan_set p where t.pan = p.pan and p.set_id = 'AI' and t.authorizer = 'AUTHH'
"""

tranSet_C = "select "+column_str+"""
from transaction_vw t, pan_set p where t.pan = p.pan and p.set_id in ('BI0S','AI') and t.authorizer = 'AUTHH'
"""

tranSet_D = "select "+column_str+"""
from transaction_vw t, pan_set p where t.pan = p.pan and p.set_id in ('BI0L','AI') and t.authorizer = 'AUTHH'
"""

def read_sql_complete(query, engine, chunk_size=100000):
    df_list = []
    for chunk in pd.read_sql(query, engine, chunksize=chunk_size):
        df_list.append(chunk)

    if df_list:
        complete_df = pd.concat(df_list, ignore_index=True)
    else:
        complete_df = pd.DataFrame()
    return complete_df

def undersample_data(df, fraud_label_column, desired_ratio=1):
    fraud_df = df[df[fraud_label_column] == 1]
    non_fraud_df = df[df[fraud_label_column] == 0]

    num_fraud = len(fraud_df)
    num_non_fraud_to_retain = int(num_fraud * desired_ratio)

    if len(non_fraud_df) > num_non_fraud_to_retain:
        non_fraud_df = non_fraud_df.sample(n=num_non_fraud_to_retain, random_state=42)

    balanced_df = pd.concat([fraud_df, non_fraud_df]).reset_index(drop=True)

    return balanced_df

def oversample_with_smote(df, target_column, sampling_strategy='auto'):
    X = df.drop(columns=[target_column])
    y = df[target_column]

    smote = SMOTE(sampling_strategy=sampling_strategy, random_state=42)
    X_res, y_res = smote.fit_resample(X, y)

    resampled_df = pd.DataFrame(X_res, columns=X.columns)
    resampled_df[target_column] = y_res

    return resampled_df

def save_dataframe_to_disk(df, filename):
    with open(filename, 'wb') as file:
        pickle.dump(df, file)

def load_dataframe_from_disk(filename):
    with open(filename, 'rb') as file:
        df = pickle.load(file)
    return df

def create_dataframe_from_columns(df, column_list):
    column_list = [col for col in column_list if col in df.columns]
    new_df = df[column_list]
    return new_df
