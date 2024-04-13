import sys
from etl.transformacionf import create_database
from transformacion import Transformacion

def main() :
    create_database()
    transf = Transformacion()
    print(transf)
    return 0

if __name__ == '__main__':
    sys.exit(main()) 
