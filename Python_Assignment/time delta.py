from datetime import datetime

if __name__ == '__main__':
    t = int(input())
    for i in range(t):
        t1 = input()
        t2 = input()
        time1 = datetime.strptime(t1, "%a %d %b %Y %H:%M:%S %z")
        time2 = datetime.strptime(t2, "%a %d %b %Y %H:%M:%S %z")
        print(abs(int((time1 - time2).total_seconds())))
