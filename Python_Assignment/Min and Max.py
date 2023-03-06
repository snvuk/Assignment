
N, M= [int (i) for i in input().split()]
arr=[]
for i in range(N):
    arr.append( [int(x) for x in input().split()]  )
ma=arr[0][0]
for i in range(N):
    mi=arr[i][0]
    for j in range(1,M):
        if (arr[i][j]<mi):
            mi=arr[i][j]
    if (mi>ma):
        ma=mi
print(ma)