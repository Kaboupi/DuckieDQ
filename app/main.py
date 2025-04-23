from DuckieDQ.DuckieDQ import DuckieDQ

import argparse

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--path', default='/', help='directiry with YAML/YML configs for DQ')
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_args()
    path = args.path
    
    duckie = DuckieDQ(path)
    duckie.execute_dq()