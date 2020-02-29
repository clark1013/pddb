""" 数据库内核demo, 原文地址: https://www.infoq.cn/theme/46 """
import csv


def csvToRowSet(filepath):
    with open(filepath, "r") as f:
        reader = csv.reader(f)
        rows = []
        for rd in reader:
            cells = [Cell(0, 0, r) for r in rd]
            rows.append(Row(cells))
        rowset = RowSet(rows)
    return rowset


#########
# 数据集 #
#########
class Cell:
    def __init__(self, _type, size, val):
        self.type = _type
        self.size = size
        self.val = val


class Row:
    def __init__(self, cells):
        self.cells = cells


class RowSet:
    def __init__(self, rows):
        self.rows = rows


class DatabaseEngine:
    def __init__(self):
        pass

    def readTableFromFile(self, filepath):
        return csvToRowSet(filepath)


#########
# 操作符 #
#########
class UnaryOperator:
    """ 一元操作符基类 """

    def __init__(self, childOperator):
        self.childOperator = childOperator

    def execute(self, rowSet):
        if self.childOperator:
            return self.__impl(self.childOperator.execute(rowSet))
        return self.__impl(rowSet)

    def __impl(self, rowSet):
        pass


class LimitOperator(UnaryOperator):
    """ 分页 """

    def __init__(self, limit, childOperator):
        self.limit = limit
        super().__init__(childOperator)

    def __impl(self, rowSet):
        return rowSet[: self.limit]


class RowComputeOperator:
    """ 给一行计算新列 """

    def __init__(self, computeCellVal):
        self.computeCellVal = computeCellVal

    def process(self, row):
        row.cells.append(self.computeCellVal(row))
        return row


class SelectionOperator:
    """ 根据索引选择列 """

    def __init__(self, indexs):
        self.indexs = indexs

    def process(self, row):
        return Row([row.cells[i] for i in self.indexs])


class ProjectionOperator(UnaryOperator):
    """ 关系映射 """

    def __init__(self, projectionOperators, childOperator):
        self.projectionOperators = projectionOperators
        super().__init__(childOperator)

    def __impl(self, rowSet):
        newRows = []
        for row in rowset.rows:
            for op in self.projectionOperators:
                row = op.process(row)
            newRows.append(row)
        return RowSet(newRows)


class FilterOperator(UnaryOperator):
    """ 筛选 """

    def __init__(self, filterColIndex, childOperator):
        self.filterColIndex = filterColIndex
        super().__init__(childOperator)

    def __impl(self, rowSet):
        newRows = []
        for row in rowSet.rows:
            if row.cells[self.filterColIndex].val:
                newRows.append(row)
        return RowSet(newRows)


class SortOperator(UnaryOperator):
    """ 排序 """

    def __init__(self, sortIndex, childOperator):
        self.sortIndex = sortIndex
        super().__init__(childOperator)

    def __impl(self, rowSet):
        newRows = sorted(rowSet.rows, key=lambda cell: cell[sortIndex])
        return RowSet(newRows)

