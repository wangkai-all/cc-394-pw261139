"""过滤表达式：词法分析、递归下降解析、否定范式(NNF)与区间裁剪判定。

语法（大小写不敏感关键字）::

    expr      := or_expr
    or_expr   := and_expr (OR and_expr)*
    and_expr  := not_expr (AND not_expr)*
    not_expr  := NOT not_expr | primary
    primary   := '(' expr ')' | predicate
    predicate := IDENT op value
               | IDENT BETWEEN value AND value
               | IDENT NOT BETWEEN value AND value
               | IDENT IN '(' value (',' value)* ')'
               | IDENT NOT IN '(' value (',' value)* ')'
               | IDENT IS [NOT] NULL
    op        := '=' | '!=' | '<>' | '<' | '<=' | '>' | '>='
    value     := 数字 | 单引号字符串 | TRUE | FALSE | NULL

解析后立即转成 NNF（否定下推并翻转算子），因此裁剪阶段不存在 NOT 节点。

三值判定 ``node_could_be_true`` 基于区域(zone)查询接口::

    has_value(col) -> bool          区域内该列是否存在任何非空值
    could_equal(col, v) -> bool
    could_compare(col, op, v) -> bool   op ∈ <, <=, >, >=, !=
    in_distinct(col, values) -> bool     精确值集合可否命中
    could_be_null(col) -> bool

任何信息缺失（统计不可信/列未跟踪）一律返回 True —— 只允许多读，不允许漏读。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional, Protocol

# --------------------------------------------------------------------- 词法
_KEYWORDS = {
    "and", "or", "not", "between", "in", "is", "null", "true", "false",
}


@dataclass
class Token:
    kind: str
    value: Any
    pos: int


class FilterParseError(ValueError):
    pass


def tokenize(text: str) -> list[Token]:
    tokens: list[Token] = []
    i, n = 0, len(text)
    while i < n:
        ch = text[i]
        if ch.isspace():
            i += 1
            continue
        if ch == "(":
            tokens.append(Token("LPAREN", "(", i)); i += 1; continue
        if ch == ")":
            tokens.append(Token("RPAREN", ")", i)); i += 1; continue
        if ch == ",":
            tokens.append(Token("COMMA", ",", i)); i += 1; continue
        if ch in "=<>!":
            if ch in "<>" and i + 1 < n and text[i + 1] == "=":
                tokens.append(Token("OP", ch + "=", i)); i += 2; continue
            if ch == "!" and i + 1 < n and text[i + 1] == "=":
                tokens.append(Token("OP", "!=", i)); i += 2; continue
            if ch == "=":
                tokens.append(Token("OP", "=", i)); i += 1; continue
            if ch in "<>":
                tokens.append(Token("OP", ch, i)); i += 1; continue
            raise FilterParseError(f"位置 {i} 处存在孤立的 '!'")
        if ch == "'":
            j = i + 1
            buf: list[str] = []
            while j < n:
                if text[j] == "'":
                    if j + 1 < n and text[j + 1] == "'":
                        buf.append("'"); j += 2; continue
                    break
                buf.append(text[j]); j += 1
            else:
                raise FilterParseError(f"位置 {i} 处字符串未闭合")
            tokens.append(Token("STRING", "".join(buf), i))
            i = j + 1
            continue
        if ch.isdigit() or (ch == "-" and i + 1 < n and text[i + 1].isdigit()):
            j = i + 1
            while j < n and (text[j].isdigit() or text[j] == "."):
                j += 1
            raw = text[i:j]
            try:
                val: Any = float(raw) if "." in raw else int(raw)
            except ValueError as e:
                raise FilterParseError(f"位置 {i} 处数字非法: {raw}") from e
            tokens.append(Token("NUMBER", val, i))
            i = j
            continue
        if ch.isalpha() or ch == "_":
            j = i + 1
            while j < n and (text[j].isalnum() or text[j] == "_"):
                j += 1
            word = text[i:j]
            low = word.lower()
            if low == "true":
                tokens.append(Token("BOOL", True, i))
            elif low == "false":
                tokens.append(Token("BOOL", False, i))
            elif low == "null":
                tokens.append(Token("NULL", None, i))
            elif low in _KEYWORDS:
                tokens.append(Token("KW", low, i))
            else:
                tokens.append(Token("IDENT", word, i))
            i = j
            continue
        raise FilterParseError(f"位置 {i} 处字符无法识别: {ch!r}")
    tokens.append(Token("EOF", None, n))
    return tokens


# --------------------------------------------------------------------- AST
@dataclass
class Node:
    kind: str                     # and | or | cmp | between | in | isnull
    col: Optional[str] = None
    op: Optional[str] = None      # = != < <= > >=
    value: Any = None
    values: Optional[list[Any]] = None
    negated: bool = False         # NOT BETWEEN / NOT IN / IS NOT NULL
    children: list["Node"] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"kind": self.kind}
        if self.col is not None:
            d["col"] = self.col
        if self.op is not None:
            d["op"] = self.op
        if self.value is not None:
            d["value"] = self.value
        if self.values is not None:
            d["values"] = self.values
        if self.kind in ("isnull", "between", "in"):
            d["negated"] = self.negated
        if self.children:
            d["children"] = [c.to_dict() for c in self.children]
        return d


# --------------------------------------------------------------------- 解析
class _Parser:
    def __init__(self, text: str):
        self.tokens = tokenize(text)
        self.pos = 0

    def peek(self) -> Token:
        return self.tokens[self.pos]

    def next(self) -> Token:
        t = self.tokens[self.pos]
        self.pos += 1
        return t

    def expect_kw(self, kw: str) -> None:
        t = self.peek()
        if t.kind != "KW" or t.value != kw:
            raise FilterParseError(f"位置 {t.pos} 处期望关键字 {kw.upper()}")
        self.next()

    def accept_kw(self, kw: str) -> bool:
        t = self.peek()
        if t.kind == "KW" and t.value == kw:
            self.next()
            return True
        return False

    def parse(self) -> Node:
        node = self.parse_or()
        if self.peek().kind != "EOF":
            t = self.peek()
            raise FilterParseError(f"位置 {t.pos} 处存在多余 token: {t.value!r}")
        return node

    def parse_or(self) -> Node:
        nodes = [self.parse_and()]
        while self.accept_kw("or"):
            nodes.append(self.parse_and())
        if len(nodes) == 1:
            return nodes[0]
        return Node(kind="and" if False else "or", children=nodes)

    def parse_and(self) -> Node:
        nodes = [self.parse_not()]
        while self.accept_kw("and"):
            nodes.append(self.parse_not())
        if len(nodes) == 1:
            return nodes[0]
        return Node(kind="and", children=nodes)

    def parse_not(self) -> Node:
        if self.accept_kw("not"):
            inner = self.parse_not()
            return negate(inner)
        return self.parse_primary()

    def parse_primary(self) -> Node:
        t = self.peek()
        if t.kind == "LPAREN":
            self.next()
            node = self.parse_or()
            rt = self.peek()
            if rt.kind != "RPAREN":
                raise FilterParseError(f"位置 {rt.pos} 处期望右括号")
            self.next()
            return node
        if t.kind != "IDENT":
            raise FilterParseError(f"位置 {t.pos} 处期望列名")
        col = self.next().value
        return self.parse_predicate(col)

    def parse_value(self) -> Any:
        t = self.peek()
        if t.kind in ("NUMBER", "STRING", "BOOL", "NULL"):
            self.next()
            return t.value
        raise FilterParseError(f"位置 {t.pos} 处期望字面量值")

    def parse_predicate(self, col: str) -> Node:
        t = self.peek()
        if t.kind == "OP":
            op = self.next().value
            val = self.parse_value()
            return Node(kind="cmp", col=col, op=op, value=val)
        if t.kind == "KW" and t.value == "between":
            self.next()
            low = self.parse_value()
            self.expect_kw("and")
            high = self.parse_value()
            return Node(kind="between", col=col, value=low, values=[low, high])
        if t.kind == "KW" and t.value == "in":
            self.next()
            lp = self.peek()
            if lp.kind != "LPAREN":
                raise FilterParseError(f"位置 {lp.pos} 处 IN 后期望 '('")
            self.next()
            vals = [self.parse_value()]
            while self.peek().kind == "COMMA":
                self.next()
                vals.append(self.parse_value())
            if self.peek().kind != "RPAREN":
                rt = self.peek()
                raise FilterParseError(f"位置 {rt.pos} 处期望 ')'")
            self.next()
            return Node(kind="in", col=col, values=vals)
        if t.kind == "KW" and t.value == "is":
            self.next()
            neg = self.accept_kw("not")
            nt = self.peek()
            if nt.kind != "NULL":
                raise FilterParseError(f"位置 {nt.pos} 处期望 NULL")
            self.next()
            return Node(kind="isnull", col=col, negated=neg)
        # NOT BETWEEN / NOT IN
        if t.kind == "KW" and t.value == "not":
            self.next()
            nxt = self.peek()
            if nxt.kind == "KW" and nxt.value == "between":
                self.next()
                low = self.parse_value()
                self.expect_kw("and")
                high = self.parse_value()
                return Node(
                    kind="between", col=col, values=[low, high], negated=True
                )
            if nxt.kind == "KW" and nxt.value == "in":
                self.next()
                if self.peek().kind != "LPAREN":
                    raise FilterParseError("NOT IN 后期望 '('")
                self.next()
                vals = [self.parse_value()]
                while self.peek().kind == "COMMA":
                    self.next()
                    vals.append(self.parse_value())
                if self.peek().kind != "RPAREN":
                    raise FilterParseError("期望 ')'")
                self.next()
                return Node(kind="in", col=col, values=vals, negated=True)
        raise FilterParseError(f"位置 {t.pos} 处期望比较算子/BETWEEN/IN/IS")


def _flip(op: str) -> str:
    return {"=": "!=", "!=": "=", "<": ">=", "<=": ">", ">": "<=", ">=": "<"}[op]


def negate(node: Node) -> Node:
    """否定下推（德摩根 + 算子翻转），输出 NNF。"""
    if node.kind == "and":
        return Node(kind="or", children=[negate(c) for c in node.children])
    if node.kind == "or":
        return Node(kind="and", children=[negate(c) for c in node.children])
    if node.kind == "cmp":
        return Node(kind="cmp", col=node.col, op=_flip(node.op), value=node.value)
    if node.kind == "between":
        return Node(
            kind="between", col=node.col, values=list(node.values or []),
            negated=not node.negated,
        )
    if node.kind == "in":
        return Node(
            kind="in", col=node.col, values=list(node.values or []),
            negated=not node.negated,
        )
    if node.kind == "isnull":
        return Node(kind="isnull", col=node.col, negated=not node.negated)
    raise FilterParseError("无法识别的 AST 节点")


def parse_filter(text: str) -> Node:
    if not text or not text.strip():
        raise FilterParseError("过滤表达式为空")
    return _Parser(text).parse()


# --------------------------------------------------------------------- 行求值
def _cmp_scalar(a: Any, op: str, b: Any) -> bool:
    if a is None or b is None:
        return False
    try:
        if op == "=":
            return a == b
        if op == "!=":
            return a != b
        return {"<": a < b, "<=": a <= b, ">": a > b, ">=": a >= b}[op]
    except TypeError:
        return False  # 类型不可比 -> 谓词为假（SQL 风格）


def eval_node(node: Node, row: dict[str, Any]) -> bool:
    if node.kind == "and":
        return all(eval_node(c, row) for c in node.children)
    if node.kind == "or":
        return any(eval_node(c, row) for c in node.children)
    v = row.get(node.col)
    if node.kind == "cmp":
        return _cmp_scalar(v, node.op, node.value)
    if node.kind == "isnull":
        is_null = v is None
        return (not is_null) if node.negated else is_null
    if node.kind == "between":
        low, high = node.values  # type: ignore[misc]
        if v is None:
            return False
        try:
            inside = low <= v <= high
        except TypeError:
            inside = False
        return (not inside) if node.negated else inside
    if node.kind == "in":
        if v is None:
            return False
        try:
            hit = v in (node.values or [])
        except TypeError:
            hit = False
        return (not hit) if node.negated else hit
    raise FilterParseError(f"未知节点类型 {node.kind}")


# --------------------------------------------------------------------- 区域接口
class Zone(Protocol):
    def has_value(self, col: str) -> bool: ...
    def could_equal(self, col: str, value: Any) -> bool: ...
    def could_compare(self, col: str, op: str, value: Any) -> bool: ...
    def in_distinct(self, col: str, values: list[Any]) -> bool: ...
    def has_value_other_than(self, col: str, value: Any) -> bool: ...
    def distinct_is_complete(self, col: str) -> bool: ...
    def get_distinct(self, col: str) -> Optional[list[Any]]:
        """完整 distinct 集合；不完整/未知时返回 None。"""
        ...
    def could_be_null(self, col: str) -> bool: ...


def node_could_be_true(node: Node, zone: Zone) -> bool:
    """NNF 节点在给定区域内是否可能为真。信息不足一律 True（保守多读）。"""
    if node.kind == "and":
        return all(node_could_be_true(c, zone) for c in node.children)
    if node.kind == "or":
        return any(node_could_be_true(c, zone) for c in node.children)

    col = node.col
    assert col is not None
    if node.kind == "isnull":
        if node.negated:  # IS NOT NULL：区域内存在非空值即可
            return zone.has_value(col)
        return zone.could_be_null(col)
    if node.kind == "cmp":
        assert node.op is not None
        if node.op == "=":
            return zone.could_equal(col, node.value)
        if node.op == "!=":
            # v != x 为真需要：存在与 x 不同的非空值；NULL 行不算（NULL != x 不为真）
            if not zone.has_value(col):
                return False
            if not zone.could_equal(col, node.value):
                return True  # 区域内根本不可能出现 x
            return zone.has_value_other_than(col, node.value)
        # < <= > >=：交给区域的 min/max 判定
        return zone.could_compare(col, node.op, node.value)
    if node.kind == "between":
        low, high = node.values  # type: ignore[misc]
        if node.negated:
            # NOT BETWEEN：无法用 min/max 排除（两侧外部），保守
            return True
        # v >= low AND v <= high
        return zone.could_compare(col, ">=", low) and zone.could_compare(
            col, "<=", high
        )
    if node.kind == "in":
        vals = node.values or []
        if node.negated:
            # NOT IN (...)：区域内存在"非空且不在集合内"的值即可能为真。
            # 可裁剪的唯一情形：distinct 精确完整，且每个 distinct 值都在集合内。
            if not zone.has_value(col):
                return False
            distinct = zone.get_distinct(col)
            if distinct is None:
                return True  # 集合不完整/信息不足 -> 保守保留
            target = set(vals)
            return any(v not in target for v in distinct)
        return zone.in_distinct(col, vals)
    raise FilterParseError(f"未知节点类型 {node.kind}")
