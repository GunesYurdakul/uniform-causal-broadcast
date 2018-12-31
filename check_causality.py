
num_processes=5
causal_map = {}
dependencies ={1:[4,5],2:[1],3:[1,2],4:[],5:[3,4]}

for i in [1,3,5]:
    with open('da_proc_'+str(i)+'.out') as f:
        content = f.readlines()
    delivered=[]
    for line in content:
        tokens = line.split(' ')

        if tokens[0] == 'b':
            if tokens[0]:
                causal_map[(i,int(tokens[1]))]=delivered.copy()
        if tokens[0] == 'd':
            if int(tokens[1]) in dependencies[i]:
                delivered.append((int(tokens[1]),int(tokens[2])))

for i in [1,3,5]:
    with open('da_proc_'+str(i)+'.out') as f:
        content = f.readlines()
    delivered=[]
    for line in content:
        tokens = line.split(' ')
        if tokens[0] == 'd':
            # if ((int(tokens[1]),int(tokens[2]))==(1,2)):
            #     print(causal_map)
            #     print('NOT CORRECT',i,(int(tokens[1]),int(tokens[2])), delivered)

            result = all(elem in delivered for elem in causal_map[(int(tokens[1]),int(tokens[2]))] )
            if result is False:
                for elem in causal_map[(int(tokens[1]), int(tokens[2]))]:
                    if elem not in delivered:
                        print('not in delivered', elem)
                print(causal_map[(int(tokens[1]), int(tokens[2]))])
                print('NOT CORRECT',i,(int(tokens[1]),int(tokens[2])), delivered)
                exit()

            delivered.append((int(tokens[1]),int(tokens[2])))

print("CORRECT")
