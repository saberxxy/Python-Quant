#-*- coding=utf-8 -*-
#遗传算法、进化算法

from deap import tools

def main():
    # 杂交函数测试
    ind1 = [1, 1, 1, 1, 1]
    ind2 = [0, 0, 0, 0, 0]
    print(ind1, ind2)

    """交换从任意位置开始并且到结尾的一段基因"""
    n1, n2 = tools.cxOnePoint(ind1, ind2)
    print(n1, n2)

    """交换连续的一段长度随机，位置随机的基因"""
    n3, n4 = tools.cxTwoPoint(ind1, ind2)
    print(n3, n4)

    """进行min(len(ind1), len(ind2))次杂交，每轮交换前产生一个随机数a，若a小于indpb则交换该轮次位置的两个基因，否则不执行交换"""
    n5, n6 = tools.cxUniform(ind1, ind2, indpb=0.5)
    print(n5, n6)


if __name__ == '__main__':
    main()

