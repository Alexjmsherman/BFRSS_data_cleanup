import pandas as pd
from collections import defaultdict

data_dict_2014 = r'C:\Users\alsherman\Desktop\Data\BRFSS\brfss_data_dictionary_2014.txt'
codebook_file = r'C:\Users\alsherman\Desktop\Data\BRFSS\brfss_codebook_2014.txt'
dataframe_pickle = r'C:\Users\alsherman\Desktop\Data\BRFSS\raw_dataframe_pickle.pkl'
data_file = r'C:\Users\alsherman\Desktop\\Data\BRFSS\LLCP2014.ASC'
output_csv = r'C:\Users\alsherman\Desktop\Data\BRFSS\brfss_cleaned_data_2014.csv'


def main():
    data_dict = initialize_data_dict(data_dict_file=data_dict_2014)
    codebook_data = get_codebook_data(codebook_file=codebook_file)
    extract_key_values_from_codebook(codebook_data=codebook_data,
                                     data_dict=data_dict)
    #data_dict_values = read_brfss_ascii_into_dict(data_file=data_file,
    #                                              data_dict=data_dict)
    # currently reading from a pickle
    #df = create_brfss_encoded_values_dataframe(data_dict_values=data_dict_values)
    #decode_brfss_data(df=df, data_dict=data_dict)
    #create_brfss_csv(df=df)


def initialize_data_dict(data_dict_file):
    """ create a dictionary with the fields from the provided data dictionary 
    
    ARGS: data_dict_file = survey field names and locations

    RETURNS: data_dict with start and end character locations in the ascii file
             and an empty dict to store key/value survey answers for each field 
    """

    data_dict_table = pd.read_table(data_dict_file, sep='\t')
    
    data_dict = {}
    for ind, row in data_dict_table.iterrows():
        start_pos = int(row['Starting Column'] - 1) # convert to 0-based index 
        end_pos = int(start_pos + row['Field Length'])
        data_dict[row['Variable Name']] = {'start_position':start_pos,
                                           'end_position':end_pos,
                                           'codebook':{}}
    return data_dict


def get_codebook_data(codebook_file):
    """ create a list of all text in the BRFSS codebook report
    
    NOTES: the original codebook is a PDF: 
    (e.g. http://www.cdc.gov/brfss/annual_data/2014/pdf/codebook14_llcp.pdf) 

    ARGS: codebook_file = .txt version of the text from the codebook PDF
    
    RETURNS: codebook_data = list of codebook text with key/value survey answers
    """
    with open(codebook_file, 'rU') as f:
        # do not remove newlines yet - used later to identify extraneous text 
        codebook_data = [row for row in f]
    return codebook_data


def extract_key_values_from_codebook(codebook_data, data_dict): 
    """
    NOTES: due to the unstructured nature of the codebook, this function 
           includes (too) many conditions to skip rows with extraneous text.
           The data_dict which holds the extracted key/value pairs extracts
           excess data that do not exist in the ascii file; however, 
           these fields are never used when replacing the values in the ascii
           field, so they can be ignored.
           
    ARGS: codebook_data = list of codebook file text with key/value to extract
          data_dict = includes codebook to store key/value from code_book data
    """
    add_row = False # notates values that span multiple rows
    ascii_character_location = None # character location of field in ascii file
    concatenate_description = False # identify descriptions that span many lines
    description = '' # current field descriptions
    previous_row = None # next row in file determines whether to add data
    current_field_name = None
    next_row_has_key_val_data = False 
    span_row = ''
    
    for row in codebook_data:
        current_row = row.strip()
        
        # identify when to start adding key value pairs to the data_dict
        if 'Value Value Label' in current_row: 
            next_row_has_key_val_data = True
            concatenate_description = False # end description spanning many lines        
            previous_row = current_row
            continue
    
        if stop_collecting_key_val_data(current_row):
            next_row_has_key_val_data = False
            continue
        
        # get field names and field character locations in the ascii data file
        if 'Column:' in current_row:        
            current_field_name, ascii_character_location = \
                get_field_name_and_ascii_location(current_row)
                
        # get the field description
        description, concatenate_description = \
            get_field_description(
                    current_row=current_row,
                    concatenate_description=concatenate_description,
                    description=description)

        if next_row_has_key_val_data:
            # check if this is the last key/val for the current field
            next_row_has_key_val_data, add_row = \
            check_if_last_key_val_for_field(
                    current_row=current_row,
                    previous_row=previous_row,
                    next_row_has_key_val_data=next_row_has_key_val_data,
                    add_row=add_row)
            # check for extraneous text and skip over these rows    
            if extraneous_text_to_skip(previous_row):
                previous_row = current_row
                continue
  
            # identify & concatenate values that span many rows to previous_row
            (more_values_to_concatenate, 
            previous_row, 
            span_row, 
            add_row, 
            next_row_has_key_val_data) = \
                concatenate_values_that_span_rows(
                        current_row=current_row,
                        previous_row=previous_row,
                        span_row=span_row,
                        add_row=add_row,
                        next_row_has_key_val_data=next_row_has_key_val_data)

            # continue in the loop until all value rows have been concatenated
            # to previous_row
            if more_values_to_concatenate:
                continue
            
            # now that the entire value is in one row, remove all but key/value
            row_without_extra_right_cols = previous_row.rsplit(' ', 3)[0]
            previous_row = ''

            # check for values that exist as a single number in a range
            # for instance number of shots = 0 - 99, value is discrete in the range         
            if 'Number of' in row_without_extra_right_cols:
                previous_row = current_row            
                continue
            row_split_by_first_space = row_without_extra_right_cols.split(' ', 1)
            key = row_split_by_first_space[0].strip()        
    
            # clean values - remove extraneous text and decode                
            if len(row_split_by_first_space) > 1:            
                val = row_split_by_first_space[1:][0].strip()
            else:
                previous_row = current_row
                continue
            if 'Go to' in val:
                val = val.split('Go to')[0]
                
            val = val.decode('utf-8', 'ignore')
            
            # clean keys remove extraneous text and switch BlANK for ' '
            if key == 'BLANK':
                key = ' '
                val = 'Not asked or Missing'
                        
            # check if variable exists in dict yet      
            if current_field_name not in data_dict:
                data_dict[current_field_name] = {'start_position':None,
                                                'end_position':None,
                                                'codebook':{}}
            
            data_dict[current_field_name]['codebook'][key] = val.strip()
            # if at least one option is two digits, then all one digit answers 
            # are appended with a zero e.g. 3 become 03            
            if len(key) == 1:
                key = '0' + key
                data_dict[current_field_name]['codebook'][key] = val.strip()
            data_dict[current_field_name]['description'] = description.strip()
            data_dict[current_field_name]['ascii_character_location'] = ascii_character_location.strip()
        previous_row = current_row


def stop_collecting_key_val_data(current_row): 
    """ identify common indicators that key/value for a field are complete """
    type_condition = ' Type: ' in current_row # previous row is field name
    # entire field is hidden (e.g. PII) and should be skipped
    hidden_condition = 'HIDDEN Data not displayed' in current_row 
    stop_collecting_key_val_data = type_condition or hidden_condition
    return stop_collecting_key_val_data
    
    
def get_field_name_and_ascii_location(current_row):
    """ get field names and field character locations in the ascii data file """
    # ascii character location (e.g. 1 - 10) 
    # located inbetween 'Column: ' and 'SAS Variable Name'
    ascii_character_location = current_row.split('Column: ')[1]
    ascii_character_location.split(' SAS Variable Name')[0].strip()
    # current field name is call the SAS Variable name in the raw data
    current_field_name = current_row.split('SAS Variable Name: ')[1].strip()
    return current_field_name, ascii_character_location
    

def get_field_description(current_row, concatenate_description, description):
    """ get the field description, which may span many lines in codebook_data

    ARGS: current row
          concatenate_description = True, if description spans many rows
          description = current description from previous row(s)

    RETURNS: description = updated description
             concatenate_description = True, if description spans many rows             
    """   
    
    # determine if the row contains a field description    
    description_condition = 'Description: ' in current_row
    concatenation_condition = concatenate_description == True
    description_in_row = description_condition or concatenation_condition 
    
    if description_in_row:
        if concatenate_description:
            #  concatenate descriptions that span many lines                        
            description += ' ' + current_row.decode('utf-8', 'ignore')
            # value should be the next item after description, warn otherwise            
            err_msg = """Error: non-description row may be adding to 
            the description. current description = {0}\n row being added = {1}.
            """.format(description, current_row) 
            # Value doesn't contain a colon. Most other items do (e.g. Column:)
            assert(':' not in current_row.split(' ')[0][0])           
        else:   
            # first row of the description            
            description = current_row.split('Description: ')[1].strip()
            description = description.decode('utf-8', 'ignore')                    
            concatenate_description = True # used for descriptions spanning many lines
    return description, concatenate_description
    

def check_if_last_key_val_for_field(
    current_row, previous_row, next_row_has_key_val_data, add_row):    
    """ check if this is the last key/val for the current field """
    if 'BEHAVIORAL RISK FACTOR SURVEILLANCE SYSTEM' in current_row:        
        next_row_has_key_val_data = False # notates the end of a page
    if 'BLANK' in previous_row:
        next_row_has_key_val_data = False # blank is always list as the last answer option
        add_row = True # do not check if key spans rows
    return next_row_has_key_val_data, add_row
    

def extraneous_text_to_skip(previous_row):
    """ check for extraneous text and skip over these rows 

    ARGS: previous row
    
    RETURNS: extraneous_text_to_skip = boolean indicating to skip row
    """

    # stop conditions indicating a row to skip
    # for Weighted & Percentage confirm there are no other words in the sentence
    stop1 = 'Weighted' in previous_row and len(previous_row) == 8 
    stop2 = 'Percentage' in previous_row and len(previous_row) == 10
    stop3 = previous_row is None
    stop4 = 'Notes:' in previous_row
    stop5 = 'is coded ' in previous_row
    
    extraneous_text_to_skip = stop1 or stop2 or stop3 or stop4 or stop5

    return extraneous_text_to_skip
    

def concatenate_values_that_span_rows(current_row, previous_row, span_row, add_row, next_row_has_key_val_data):
    """ identify and concatenate values that span many rows to previous_row
    
    Notes: The key/value rows contain five items: 
      1. Value 2. Value Label 3. Frequency 4. Percentage 5. Weighted Percentage
          Value = the key, used in the ascii file
          Value Label = value to replace for keys in the ascii file
      
      These values often span multiple rows. Look for the Weighted percentage
      to determine when a value set is complete.
      
      If the row has all five items, then no actions below occur
    """
    
    more_values_to_concatenate = False
    
    #  extract the last items in the row to determine if it is the weighted %   
    cols_to_remove = previous_row.rsplit(' ', 1)
    # look for a period inside of the weighted % characters (e.g. 63.35)
    # ignore periods in the last position [0:-1] as these are sentence endings
    final_row_concat_cond = '.' in cols_to_remove[-1].strip()[0:-1]                     
    
    if add_row:       
        # add_row only entered after first value row extracted or if 'BLANK'        
        add_row = False
        
        # identify rows that should not be concatenated to value
        blank_cond = 'BLANK' in previous_row # blank is the last key        
        # the add row condition is not entered for the first value so the 
        # equals sign condition does not apply to the first value row.
        # Otherwise '=' seem to only occur in a second line of extraneous text
        # in notes sections (e.g. CRACASC1 = 20 or CRACASC1 > 100)
        equal_sign_cond = '=' in previous_row
        row_should_not_concat_to_val = blank_cond or equal_sign_cond
        
        if row_should_not_concat_to_val:
            pass
        elif final_row_concat_cond:
            previous_row = span_row  + ' ' + previous_row.strip()
        else:
            # continue in the loop until final_row_catcat_cond is met to ensure
            # all value rows have been concatenated together                
            more_values_to_concatenate = True
            
            if current_row[-1] != ':': # indicates a new field descriptor
                span_row += ' ' + previous_row.strip()
                previous_row = current_row
                add_row = True                        
            else:
                next_row_has_key_val_data = False    
    elif 'Value Value Label ' in previous_row.strip():
        # avoid adding header row ('Value Value Label'..) to span_row         
        pass
    elif not final_row_concat_cond:
        # enter this condition for the first value row        
        span_row = previous_row.strip()
        add_row = True
        previous_row = current_row
        more_values_to_concatenate = True
    
    #return more_values_to_concatenate
    return more_values_to_concatenate, previous_row, span_row, add_row, next_row_has_key_val_data



def read_brfss_ascii_into_dict(data_file, data_dict):
    # add the values from the ascii file to a list in the data_dict
    data = pd.read_table(data_file, header=None)

    data_dict_values = defaultdict(list)
    for ind, item in data.iterrows():
        for key in data_dict:
            start_pos = data_dict[key]['start_position']
            end_pos = data_dict[key]['end_position']
            val = item[0][start_pos:end_pos]
            data_dict_values[key].append(val)   
    return data_dict_values


def create_brfss_encoded_values_dataframe(data_dict_values):
    # df = pd.DataFrame(data_dict_values)
    # df.to_pickle(dataframe_pickle )
    df = pd.read_pickle(dataframe_pickle)
    return df


def decode_brfss_data(df, data_dict):
    fields_in_df = []    
    for field in ['_STATE', 'HADMAM', 'HOWLONG', 'PROFEXAM', 'LENGEXAM', 'HADPAP2', 'LASTPAP2', 'HADHYST2', 'PCPSAAD2',	'PCPSADI1',	'PCPSAAD2',	'PCPSADI1',	'PCPSARE1',	'PSATEST1',	'PSATIME',	'PCPSARS1',	'BLDSTOOL',	'LSTBLDS3',	'HADSIGM3',	'HADSGCO1',	'LASTSIG3',	'HPVTEST',	'HPLSTTST',	'HPVADVC2',	'HPVADSHT',	'EDUCA',	'_IMPEDUC',	'INCOME2',	'_INCOMG',	'HLTHPLN1',	'PERSDOC2',	'MEDCOST',	'CHECKUP1',	'MEDICARE',	'HLTHCVR1',	'DELAYMED',	'DLYOTHER',	'NOCOV121',	'LSTCOVRG',	'DRVISITS',	'MEDSCOST',	'CARERCVD',	'MEDBILL1']:
        if field in df.columns:
            fields_in_df.append(field)

    for field in df[fields_in_df]:  
        decoded_val_list = []
        for encoded_val in df[field]:
            if encoded_val in data_dict[field]['codebook']:
                decoded_val = data_dict[field]['codebook'][encoded_val]
            else:
                decoded_val = encoded_val
            decoded_val_list.append(decoded_val)
        df[field] = decoded_val_list


def create_brfss_csv(df):
    df[fields_in_df].to_csv(output_csv, index=False)


if __name__ == "__main__": main()
"""
### VARIABLES OF INTEREST ###
breast and cervical cancer screening
data_dict['HADMAM']
data_dict['HOWLONG']
data_dict['PROFEXAM']
data_dict['LENGEXAM']
data_dict['HADPAP2']
data_dict['LASTPAP2']
data_dict['HADHYST2']

prostate cancer screening
data_dict['PCPSAAD2']
data_dict['PCPSADI1']
data_dict['PCPSARE1']
data_dict['PSATEST1']
data_dict['PSATIME']
data_dict['PCPSARS1']

colorectal cancer screening
data_dict['BLDSTOOL']
data_dict['LSTBLDS3']
data_dict['HADSIGM3']
data_dict['HADSGCO1']
data_dict['LASTSIG3']


HPV testing & vaccination 
data_dict['HPVTEST']
data_dict['HPLSTTST']
data_dict['HPVADVC2']
data_dict['HPVADSHT']


education level 
data_dict['EDUCA']
data_dict['_IMPEDUC']

income
data_dict['INCOME2']
data_dict['_INCOMG']

healthcare access
data_dict['HLTHPLN1']
data_dict['PERSDOC2']
data_dict['MEDCOST']
data_dict['CHECKUP1']
data_dict['MEDICARE']
data_dict['HLTHCVR1']
data_dict['DELAYMED']
data_dict['DLYOTHER']
data_dict['NOCOV121']
data_dict['LSTCOVRG']
data_dict['DRVISITS']
data_dict['MEDSCOST']
data_dict['CARERCVD']
data_dict['MEDBILL1']
"""


"""
            # identify and concatenate values that span multiple rows
            # look for a period in the weighted percentage to identify the end 
            cols_to_remove = previous_row.rsplit(' ', 3)
            final_row_concat_cond = '.' in cols_to_remove[-1].strip()[0:-1]                     
            
            if add_row:       
                add_row = False
                if 'BLANK' in previous_row or '=' in previous_row:
                    pass # blank is the last key - does not require concatenate
                elif final_row_concat_cond: # final concatenation condition
                    previous_row = span_row  + ' ' + previous_row.strip()
                else:
                    if current_row.strip()[-1] != ':': # new var names
                        span_row = span_row  + ' ' + previous_row.strip()
                        previous_row = current_row
                        add_row = True                        
                    else:
                        next_row_has_key_val_data = False
                    continue
            elif not final_row_concat_cond:
                span_row = previous_row.strip()
                add_row = True
                previous_row = current_row
                continue
 """