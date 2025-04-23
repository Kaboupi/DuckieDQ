from DuckieDQ.supporting.connection import Connection
from DuckieDQ.DuckieDQ import DuckieDQ

if __name__ == '__main__':
    duckie = DuckieDQ('../dq_configs')
    duckie.execute_dq()