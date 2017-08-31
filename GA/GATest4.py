#-*- coding=utf-8 -*-
# 遗传算法

import numpy as np
import matplotlib.pyplot as plt

X_BOUND = [0, 5]
DNA_SIZE = 10  # DNA的长度
POP_SIZE = 100  #种群规模
CROSS_RATE = 0.8  # 进行杂交的种群比例
MUTATION_RATE = 0.003  # 变异的概率
N_GENERATIONS = 200  # 繁衍代数


# 所求的函数
def F(x):
    return np.sin(10*x)*x + np.cos(2*x)*x

# 计算适应度
def get_fitness(pred):
    return pred + 1e-3 - np.min(pred)


# 将DNA转换为描述的内容，在此例中为二进制转换为十进制
def translateDNA(pop):
    return pop.dot(2**np.arange(DNA_SIZE)[::-1]) / (2**DNA_SIZE-1) * X_BOUND[1]


# 自然选择，适者生存
def select(pop, fitness):
    # 概率选择法
    idx = np.random.choice(np.arange(POP_SIZE), size=POP_SIZE, replace=True,
                           p=fitness / fitness.sum())
    return pop[idx]


# DNA交叉配对
def crossover(parent, pop):
    if np.random.rand() < CROSS_RATE:
        i_ = np.random.randint(0, POP_SIZE, size=1)
        cross_points = np.random.randint(0, 2, size=DNA_SIZE).astype(np.bool)
        parent[cross_points] = pop_copy[i_, cross_points]
    return parent


# DNA变异
def mutate(child):
    for point in range(DNA_SIZE):
        if np.random.rand() < MUTATION_RATE:
            child[point] = 1 if child[point] == 0 else 0
    return child

def main():
    pop = np.random.randint(0, 2, (1, DNA_SIZE)).repeat(POP_SIZE, axis=0)
    # print(pop)

    # 绘图
    plt.ion()  # something about plotting
    x = np.linspace(*X_BOUND, 200)
    plt.plot(x, F(x))


    for _ in range(N_GENERATIONS):
        F_values = F(translateDNA(pop))

        if 'sca' in globals():
            sca.remove()
        sca = plt.scatter(translateDNA(pop), F_values, s=200, lw=0, c='red', alpha=0.5)
        plt.pause(0.05)

        fitness = get_fitness(F_values)
        print("Most fitted DNA: ", pop[np.argmax(fitness), :])
        pop = select(pop, fitness)
        pop_copy = pop.copy()

        for parent in pop:
            child = crossover(parent, pop_copy)
            child = mutate(child)
            parent[:] = child

    plt.ioff()
    plt.show()


if __name__ == '__main__':
    main()

