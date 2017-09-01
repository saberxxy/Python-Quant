#-*- coding=utf-8 -*-
#遗传算法、进化算法

import random
from deap import base
from deap import creator
from deap import tools


#每个个体的评价函数，返回值为一百个基因数的总和
def evalOneMin(individual):
    return sum(individual),  #返回值加逗号，组成tuple


#为初始化进化设定参数
def setParam():
    creator.create("FitnessMin", base.Fitness, weights=(-1.0,))  #设定进化的目标，正数代表趋近于大数，负数代表趋近于小数
    creator.create("Individual", list, fitness=creator.FitnessMin)  # 个体的适应度度量
    toolbox = base.Toolbox()  # 引入工具类
    toolbox.register("attr_bool", random.randint, 0, 1)  # 基因为0和1任意的随机整数
    toolbox.register("individual", tools.initRepeat, creator.Individual, toolbox.attr_bool, 100)  #设定基因数量
    toolbox.register("population", tools.initRepeat, list, toolbox.individual)

    toolbox.register("evaluate", evalOneMin)  # 进化的评价，第二个参数为函数
    toolbox.register("mate", tools.cxTwoPoint)  # 杂交策略为交换连续的一段长度随机且位置随机的基因
    toolbox.register("mutate", tools.mutFlipBit, indpb=0.05)  # 突变策略，个体基因突变的的概率为0.05
    toolbox.register("select", tools.selTournament, tournsize=3)  #赌轮选择法，每次进行三轮选择
    return creator, toolbox


def main():
    creator, toolbox = setParam()
    pop = toolbox.population(n=500)  #设定种群数量
    # print (pop)

    NGEN = 100  #演化的代数
    CXPB = 0.5  #杂交概率
    MUTPB = 0.5  #突变概率

    fitnesses = list(map(toolbox.evaluate, pop))  #每个个体的各基因之和
    # print (fitnesses)

    for ind, fit in zip(pop, fitnesses):  #ind为list，ind.fitness.values为tuple
        ind.fitness.values = fit
        # print (ind)

    #开始演化
    for g in range(NGEN):
        print("-- Generation %i --" % g)
        # Select the next generation individuals  选择下一代
        offspring = toolbox.select(pop, len(pop))
        # Clone the selected individuals  克隆选定个体
        offspring = list(map(toolbox.clone, offspring))
        #第一代十个个体
        # [1, 0, 0, 0, 1], [1, 0, 0, 0, 1], [1, 0, 0, 1, 0], [0, 1, 1, 0, 1], [1, 0, 1, 1, 1],
        # [1, 0, 1, 1, 1], [1, 1, 1, 1, 1], [1, 1, 1, 1, 1], [1, 1, 0, 1, 1], [1, 0, 0, 0, 1]

        # Apply crossover on the offspring  后代繁衍进行杂交
        for child1, child2 in zip(offspring[::2], offspring[1::2]):  #将索引为奇数和偶数的个体分为两类
            # print (random.random())
            if random.random() < CXPB:  #在0和1之间生成随机小数，小于杂交概率则杂交
                toolbox.mate(child1, child2)  #两个个体杂交产生新的两个个体，子代个体取代亲代
                del child1.fitness.values
                del child2.fitness.values

        # Apply mutation on the offspring  后代繁衍进行变异
        for mutant in offspring:
            if random.random() < MUTPB:
                toolbox.mutate(mutant)
                del mutant.fitness.values

        # Evaluate the individuals with an invalid fitness
        invalid_ind = [ind for ind in offspring if not ind.fitness.valid]  #筛选出与亲代不相同的个体

        fitnesses = map(toolbox.evaluate, invalid_ind)
        # print (fitnesses)
        for ind, fit in zip(invalid_ind, fitnesses):
            # print (ind, fit)
            ind.fitness.values = fit

        pop[:] = offspring
        # Gather all the fitnesses in one list and print the stats  #集合进化后的种群
        fits = [ind.fitness.values[0] for ind in pop]
        print (fits)

        length = len(pop)
        mean = sum(fits) / length
        sum2 = sum(x * x for x in fits)
        std = abs(sum2 / length - mean ** 2) ** 0.5
        print("  Min %s" % min(fits))  # 最小值
        print("  Max %s" % max(fits))  # 最大值
        print("  Avg %s" % mean)  # 平均值
        print("  Std %s" % std)  # 标准差


if __name__ == '__main__':
    main()