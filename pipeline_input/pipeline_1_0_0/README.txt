README file for the DataPipeLine() class

# ============================================================================
SETUP 
1) The Data pipeline to input information for analysis is based on the following folders to be setup for individual projects:
    [project code folder]
            [input] (data pipeline, in python)
            [result] (data results, in R)
            [config] (project variables, project_variables.yaml)
    [project data folder]
    [project deploy folder]
2) The Pipeline for input and for output results are saved in the following folder on Github (development versions are saved in Nexus_dev):
    [nexus]
        [input_pipeline]
            [pipeline_N_N_N] (Version of the input pipeline)
        [result_output]
            [result_output_N_N_N] (Version of the output pipeline)
# ============================================================================
HOWTO
0) Setup steps:
    - Make sure you have the latest version from the git folder
1) copy the folder with the latest version of the module (eg. pipeline_1_0_0
    - Run "pipeline_setup_new.py" after selecting project_name and module version
3) Select the version of the module to use: sys.modules["input_pipeline.modules"] = input_pipeline.modules_N_N_N
4) Change the "Global variables" in the ./config/project_variables.yaml file
5) start setting up the pipeline by adapting the "Pipeline setup". The pipeline will run sequentially each step in the steps list.
6) run the script
7) To re-run the script some intermediary files in the data "tempdir" folder can be removed
8) Specifics: run with EC2 16Gb.
# ============================================================================
    Run individual steps in the pipeline
    Steps to run (order is very important as dependencies exist between steps)
    Usage:
    1) add as many pipeline step as needed, for instance p1.pipeline_end()
    2) run the script, for instance:
    p1.pipeline_bas()
    p1.pipeline_len()
    3) log of results is saved in the app_project_name.log file
# ============================================================================
Information about each step (method)
    bas = generate SQL table(s): baselines and taxonomies in the [project].duckdb file saved in the Data folder
        Prerequisite: None
    len = generate SQL table(s): records_dataset from Lens.org api call (with "lens_id" column and header)
        Prerequisite: bas
        Input: Search strings in text files in the [PROJECT]/search strategy folder
        Output:
            A DF pickle file in data/[PROJECT]/temp_files/[PROJECT][VARIANT].pkl
        Options:
            data_label = 'label_name' => a label name to produce different versions of the list of records
            data_label = None => default value and no label is added to the names of files and tables
    ddb = generate SQL table(s): save project data into a DB (with "uid" column and header)
        Prerequisite: len
        Input: a pandas DF with data from APIs saved as a pickle file in data/[PROJECT]/temp_files/[PROJECT][VARIANT].pkl
        Output: a duckDB file saved in data/[PROJECT]/[PROJECT][VARIANT].duckdb
        Options:
            data_label = 'label_name' => a label name to produce different versions of the list of uids
            data_label = None => default value and no label is added to the names of files and tables
            data_uid = the value of the heading for records' identifier (eg lens_id)
    [cus = customised step, generally used to customise the standard deliverable, by default empty (not in class)]
# ============================================================================
