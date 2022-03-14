from app.run import run 
import sys 
import ast 

# spark-submit main.py "{'job_name': 'get_movie_data', 'path':'s3//movie//config//file.yml'}"

if __name__ == "__main__":
    str_params = sys.argv[1]
    params = ast.literal_eval(str_params)

    is_success = run(params) 

    if is_success:
        print("Code has ran successfully")
    else:
        raise Exception("Code has unexpectedly failed")
    

