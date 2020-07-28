
print('field "params" for {}'.format(row_code), pdf['params'].values[0])

try:
    base_param=json.loads(pdf['params'].values[0]) # read saved params from db - params in JSON format
except:
    base_param={'SEASON':{"working":"none"}} # not saved params, create new - empty
    
print("="*40)
try:
    db_working_alg=base_param['SEASON']['working'] # current working method for seasonal decompose 
except:
    db_working_alg='None'
    
try:
    db_working_params=base_param['SEASON'][db_working_alg] # ... and current working params
except KeyError:
    db_working_params=''
    
print('for row ', row_code)
print('-'*40)
print('working algorithm - ', db_working_alg)
try:
    print('working params:')
    for k, v in db_working_params.items():
        print('\t{0} = {1}'.format(k, v))
    print('-'*40)
except:
    print('working params: None')
    
def init_template_params(strKeyInDB):
    try:
        dct_params=base_param['SEASON'][strKeyInDB]
    except KeyError:
        dct_params=dict()
    return dct_params

cmasf_params=init_template_params('cmasf') # empty template for cmasf=method params

stmss_params=init_template_params('mov_avg') # empty template for moving averege=method params mov_avg

stmstl_params=init_template_params('stl') # empty or db-saved template for LOESS (STL)=method params


str4def='default' # this value reset param key to default values for each methods
