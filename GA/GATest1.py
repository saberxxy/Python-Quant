#-*- coding=utf-8 -*-
#遗传算法、进化算法

import random
from deap import base
from deap import creator
from deap import tools

creator.create("FitnessMax", base.Fitness, weights=(1.0,))
creator.create("Individual", list, fitness=creator.FitnessMax)

toolbox = base.Toolbox()
# Attribute generator  属性生成器
toolbox.register("attr_bool", random.randint, 0, 1)  #生成0和1之间的随机数
# Structure initializers  初始化
toolbox.register("individual", tools.initRepeat, creator.Individual, toolbox.attr_bool, 100)  #基因数，一百个基因，由0和1随机而成
toolbox.register("population", tools.initRepeat, list, toolbox.individual)

def evalOneMax(individual):
    return sum(individual),

toolbox.register("evaluate", evalOneMax)
toolbox.register("mate", tools.cxTwoPoint)
toolbox.register("mutate", tools.mutFlipBit, indpb=0.05)  #indpb每个个体的突变独立概率
toolbox.register("select", tools.selTournament, tournsize=3)  

def main():
    pop = toolbox.population(n=300)  #个体数
    print (len(pop))
    for a in pop:
        print (len(a))
    NGEN = 2  #演化代数
    CXPB = 0.5  #杂交概率
    MUTPB = 0.5  #突变概率

    # Evaluate the entire population
    fitnesses = list(map(toolbox.evaluate, pop))
    # print (fitnesses)
    for ind, fit in zip(pop, fitnesses):
        ind.fitness.values = fit
        print (fit)
        # print (ind.fitness.values)
        # Begin the evolution
    for g in range(NGEN):
        print("-- Generation %i --" % g)
        # Select the next generation individuals  选择下一代
        offspring = toolbox.select(pop, len(pop))
        # Clone the selected individuals  克隆选定个体
        offspring = list(map(toolbox.clone, offspring))
        # Apply crossover and mutation on the offspring  后代繁衍进行交叉变异
        for child1, child2 in zip(offspring[::2], offspring[1::2]):
            if random.random() < CXPB:
                toolbox.mate(child1, child2)
                del child1.fitness.values
                del child2.fitness.values

        for mutant in offspring:
            if random.random() < MUTPB:
                toolbox.mutate(mutant)
                del mutant.fitness.values

        # Evaluate the individuals with an invalid fitness
        invalid_ind = [ind for ind in offspring if not ind.fitness.valid]
        fitnesses = map(toolbox.evaluate, invalid_ind)
        for ind, fit in zip(invalid_ind, fitnesses):
            ind.fitness.values = fit

        pop[:] = offspring

        # Gather all the fitnesses in one list and print the stats
        fits = [ind.fitness.values[0] for ind in pop]

        length = len(pop)
        mean = sum(fits) / length
        sum2 = sum(x * x for x in fits)
        std = abs(sum2 / length - mean ** 2) ** 0.5

        print("  Min %s" % min(fits))  #最小值
        print("  Max %s" % max(fits))  #最大值
        print("  Avg %s" % mean)  #平均值
        print("  Std %s" % std)  #标准差

if __name__ == '__main__':
    main()