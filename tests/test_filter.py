"""过滤表达式：解析、NNF 下推、行求值与区域裁剪判定测试。"""
import unittest

from minilake.filter import (
    FilterParseError,
    eval_node,
    node_could_be_true,
    parse_filter,
)


class FakeZone:
    """可编程的假区域：按列给定 (distinct 完整集合, 是否含 NULL)。"""

    def __init__(self, columns: dict, nulls: set | None = None):
        self.columns = columns          # col -> list[Any]，视为完整 distinct
        self.nulls = nulls or set()

    def _c(self, col):
        return self.columns.get(col)

    def has_value(self, col):
        # 未建模的列：信息不足，保守认为有非空值
        if col not in self.columns:
            return True
        return len(self.columns[col]) > 0

    def could_equal(self, col, value):
        c = self._c(col)
        if c is None:
            return True
        if not c:
            return False
        return value in c

    def could_compare(self, col, op, value):
        c = self._c(col)
        if c is None:
            return True
        if not c:
            return False
        lo, hi = min(c), max(c)
        return {
            "<": lo < value, "<=": lo <= value,
            ">": hi > value, ">=": hi >= value,
        }[op]

    def in_distinct(self, col, values):
        c = self._c(col)
        if c is None:
            return True
        if not c:
            return False
        return any(v in c for v in values)

    def has_value_other_than(self, col, value):
        c = self._c(col)
        if c is None:
            return True
        return any(v != value for v in c)

    def distinct_is_complete(self, col):
        return col in self.columns

    def get_distinct(self, col):
        return list(self.columns[col]) if col in self.columns else None

    def could_be_null(self, col):
        return col in self.nulls


class TestParser(unittest.TestCase):
    def test_basic_predicates(self):
        for text in [
            "a = 1", "a != 'x'", "a <= 3.5", "a BETWEEN 1 AND 9",
            "a NOT BETWEEN 1 AND 9", "a IN (1,2,3)", "a NOT IN ('x','y')",
            "a IS NULL", "a IS NOT NULL",
        ]:
            node = parse_filter(text)
            self.assertIsNotNone(node)

    def test_precedence_and_or_not(self):
        n = parse_filter("NOT a = 1 AND b = 2 OR c = 3")
        self.assertEqual(n.kind, "or")
        self.assertEqual(n.children[0].kind, "and")
        # NOT 已下推：a = 1 -> a != 1
        self.assertEqual(n.children[0].children[0].op, "!=")

    def test_parentheses_and_negation_normal_form(self):
        n = parse_filter("NOT (a = 1 OR b = 2)")
        self.assertEqual(n.kind, "and")  # 德摩根
        self.assertEqual(n.children[0].op, "!=")
        self.assertEqual(n.children[1].op, "!=")

    def test_not_between_flips_flag(self):
        n = parse_filter("NOT (a BETWEEN 1 AND 9)")
        self.assertEqual(n.kind, "between")
        self.assertTrue(n.negated)

    def test_parse_errors(self):
        for bad in ["", "a =", "a >", "(a = 1", "a = 1 1", "!", "NOT"]:
            with self.assertRaises(FilterParseError):
                parse_filter(bad)

    def test_string_escaping(self):
        n = parse_filter("a = 'it''s ok'")
        self.assertEqual(n.value, "it's ok")


class TestRowEval(unittest.TestCase):
    def test_truth_table(self):
        row = {"a": 5, "b": "x", "c": None}
        cases = {
            "a = 5": True, "a != 5": False, "a > 4": True, "a < 5": False,
            "a BETWEEN 1 AND 9": True, "a NOT BETWEEN 1 AND 9": False,
            "b IN ('x','y')": True, "b NOT IN ('y')": True,
            "c IS NULL": True, "c IS NOT NULL": False,
            "a IS NOT NULL": True,
            "a > 1 AND b = 'z'": False,
            "a > 1 OR b = 'z'": True,
            "NOT (a < 1)": True,
            "c = 1": False,          # NULL 比较不为真
            "c != 1": False,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                self.assertEqual(eval_node(parse_filter(text), row), expected)


class TestZonePruning(unittest.TestCase):
    def setUp(self):
        # a 的值域 [1..10] 精确完整；b 只有 'x'；
        # c 列只有 NULL（无任何非空值）；d 列非空无 NULL
        self.zone = FakeZone(
            {"a": [1, 2, 3, 10], "b": ["x"], "c": [], "d": [7]},
            nulls={"c"},
        )

    def test_zone_does_not_prunable_when_possible(self):
        # 谓词可能为真 -> 保留
        for text in [
            "a = 3", "a >= 10", "a <= 1", "a != 5",
            "a BETWEEN 2 AND 3", "b IN ('x','y')",
            "b = 'x'", "c IS NULL", "d IS NOT NULL",
        ]:
            with self.subTest(text=text):
                self.assertTrue(
                    node_could_be_true(parse_filter(text), self.zone)
                )

    def test_zone_prunes_when_impossible(self):
        for text in [
            "a = 99",            # 精确集合不含
            "a > 100",           # max=10
            "a < 0",             # min=1
            "a BETWEEN 11 AND 99",
            "b = 'y'",           # b 恒为 x
            "b != 'x'",          # 精确集合只有 x -> 不可能有其他值
            "b IN ('y','z')",
            "d IS NULL",         # d 非空、无 NULL
            "c IS NOT NULL",     # c 全 NULL（区域里没有非空值）
        ]:
            with self.subTest(text=text):
                self.assertFalse(
                    node_could_be_true(parse_filter(text), self.zone)
                )

    def test_unknown_column_is_conservative(self):
        # 未跟踪列：等值/比较谓词保守保留
        for text in ["mystery = 1", "mystery > 9"]:
            self.assertTrue(
                node_could_be_true(parse_filter(text), FakeZone({}))
            )
        # IS NULL 在"无任何行"的空区域上可证伪（区域确定为空）
        self.assertFalse(
            node_could_be_true(
                parse_filter("mystery IS NULL"),
                FakeZone({"mystery": []}),
            )
        )

    def test_and_or_zone_semantics(self):
        z = self.zone
        # AND：一支必假 -> 整体可裁剪
        self.assertFalse(
            node_could_be_true(parse_filter("a = 3 AND b = 'y'"), z)
        )
        # OR：一支可能真 -> 保留
        self.assertTrue(
            node_could_be_true(parse_filter("a = 99 OR b = 'x'"), z)
        )
        # OR：两支都必假 -> 可裁剪
        self.assertFalse(
            node_could_be_true(parse_filter("a = 99 OR b = 'y'"), z)
        )

    def test_not_in_pruning_requires_complete_distinct(self):
        z_complete = FakeZone({"b": ["x"]})
        # 完整集合且全部在 IN 列表内 -> NOT IN 必假，可裁剪
        self.assertFalse(
            node_could_be_true(parse_filter("b NOT IN ('x')"), z_complete)
        )
        # 信息不足 -> 保守保留
        self.assertTrue(
            node_could_be_true(parse_filter("b NOT IN ('x')"), FakeZone({}))
        )


if __name__ == "__main__":
    unittest.main()
