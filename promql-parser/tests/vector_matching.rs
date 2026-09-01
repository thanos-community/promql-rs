//! Tests for vector matching modifiers in binary expressions.

use promql_parser::ast::{BinaryExpr, VectorMatchCardinality};
use promql_parser::{parse_expr, Expr};

#[test]
fn bool_modifier() {
    let input = "a == bool b";
    let expr = parse_expr(input).expect("parse failed");

    if let Expr::Binary(BinaryExpr {
        return_bool,
        vector_matching,
        ..
    }) = expr
    {
        assert!(return_bool, "bool flag should be set");
        assert!(
            vector_matching.is_some(),
            "vector_matching should be present"
        );
        let vm = vector_matching.unwrap();
        assert_eq!(vm.card, VectorMatchCardinality::OneToOne);
    } else {
        panic!("Expected BinaryExpr, got {:?}", expr);
    }
}

#[test]
fn ignoring_modifier() {
    let input = "a + ignoring(foo, bar) b";
    let expr = parse_expr(input).expect("parse failed");

    if let Expr::Binary(BinaryExpr {
        vector_matching, ..
    }) = expr
    {
        let vm = vector_matching.expect("vector_matching should be present");
        assert!(!vm.on, "on should be false for ignoring");
        assert_eq!(vm.matching_labels, vec!["foo", "bar"]);
        assert_eq!(vm.card, VectorMatchCardinality::OneToOne);
    } else {
        panic!("Expected BinaryExpr, got {:?}", expr);
    }
}

#[test]
fn on_modifier() {
    let input = "a / on(instance) b";
    let expr = parse_expr(input).expect("parse failed");

    if let Expr::Binary(BinaryExpr {
        vector_matching, ..
    }) = expr
    {
        let vm = vector_matching.expect("vector_matching should be present");
        assert!(vm.on, "on should be true");
        assert_eq!(vm.matching_labels, vec!["instance"]);
        assert_eq!(vm.card, VectorMatchCardinality::OneToOne);
    } else {
        panic!("Expected BinaryExpr, got {:?}", expr);
    }
}

#[test]
fn group_left_modifier() {
    let input = "a + on(foo) group_left(bar, baz) b";
    let expr = parse_expr(input).expect("parse failed");

    if let Expr::Binary(BinaryExpr {
        vector_matching, ..
    }) = expr
    {
        let vm = vector_matching.expect("vector_matching should be present");
        assert!(vm.on, "on should be true");
        assert_eq!(vm.matching_labels, vec!["foo"]);
        assert_eq!(vm.card, VectorMatchCardinality::ManyToOne);
        assert_eq!(vm.include, vec!["bar", "baz"]);
    } else {
        panic!("Expected BinaryExpr, got {:?}", expr);
    }
}

#[test]
fn group_right_modifier() {
    let input = "a * ignoring(cpu) group_right b";
    let expr = parse_expr(input).expect("parse failed");

    if let Expr::Binary(BinaryExpr {
        vector_matching, ..
    }) = expr
    {
        let vm = vector_matching.expect("vector_matching should be present");
        assert!(!vm.on, "on should be false for ignoring");
        assert_eq!(vm.matching_labels, vec!["cpu"]);
        assert_eq!(vm.card, VectorMatchCardinality::OneToMany);
        assert!(
            vm.include.is_empty(),
            "include should be empty when no labels specified"
        );
    } else {
        panic!("Expected BinaryExpr, got {:?}", expr);
    }
}

#[test]
fn bool_with_on() {
    let input = "a == bool on(job) b";
    let expr = parse_expr(input).expect("parse failed");

    if let Expr::Binary(BinaryExpr {
        return_bool,
        vector_matching,
        ..
    }) = expr
    {
        assert!(return_bool, "bool flag should be set");
        let vm = vector_matching.expect("vector_matching should be present");
        assert!(vm.on, "on should be true");
        assert_eq!(vm.matching_labels, vec!["job"]);
    } else {
        panic!("Expected BinaryExpr, got {:?}", expr);
    }
}

#[test]
fn complex_modifier_chain() {
    let input = "a + bool on(instance, job) group_left(pod, namespace) b";
    let expr = parse_expr(input).expect("parse failed");

    if let Expr::Binary(BinaryExpr {
        return_bool,
        vector_matching,
        ..
    }) = expr
    {
        assert!(return_bool, "bool flag should be set");
        let vm = vector_matching.expect("vector_matching should be present");
        assert!(vm.on, "on should be true");
        assert_eq!(vm.matching_labels, vec!["instance", "job"]);
        assert_eq!(vm.card, VectorMatchCardinality::ManyToOne);
        assert_eq!(vm.include, vec!["pod", "namespace"]);
    } else {
        panic!("Expected BinaryExpr, got {:?}", expr);
    }
}

#[test]
fn empty_on_labels() {
    let input = "a + on() b";
    let expr = parse_expr(input).expect("parse failed");

    if let Expr::Binary(BinaryExpr {
        vector_matching, ..
    }) = expr
    {
        let vm = vector_matching.expect("vector_matching should be present");
        assert!(vm.on, "on should be true");
        assert!(
            vm.matching_labels.is_empty(),
            "matching_labels should be empty"
        );
    } else {
        panic!("Expected BinaryExpr, got {:?}", expr);
    }
}

#[test]
fn no_modifiers_means_no_vector_matching() {
    let input = "a + b";
    let expr = parse_expr(input).expect("parse failed");

    if let Expr::Binary(BinaryExpr {
        vector_matching,
        return_bool,
        ..
    }) = expr
    {
        assert!(!return_bool, "bool should be false");
        assert!(
            vector_matching.is_none(),
            "vector_matching should be None when no modifiers present (zero allocation)"
        );
    } else {
        panic!("Expected BinaryExpr, got {:?}", expr);
    }
}

#[test]
fn fill_modifier() {
    let input = "a + on(instance) fill(0) b";
    let expr = parse_expr(input).expect("parse failed");

    if let Expr::Binary(BinaryExpr {
        vector_matching, ..
    }) = expr
    {
        let vm = vector_matching.expect("vector_matching should be present");
        assert!(vm.on, "on should be true");
        assert_eq!(vm.matching_labels, vec!["instance"]);
        assert_eq!(vm.fill_values.lhs, Some(0.0), "lhs fill should be 0");
        assert_eq!(vm.fill_values.rhs, Some(0.0), "rhs fill should be 0");
    } else {
        panic!("Expected BinaryExpr, got {:?}", expr);
    }
}

#[test]
fn fill_left_modifier() {
    let input = "a + ignoring(cpu) fill_left(1.5) b";
    let expr = parse_expr(input).expect("parse failed");

    if let Expr::Binary(BinaryExpr {
        vector_matching, ..
    }) = expr
    {
        let vm = vector_matching.expect("vector_matching should be present");
        assert!(!vm.on, "on should be false");
        assert_eq!(vm.fill_values.lhs, Some(1.5), "lhs fill should be 1.5");
        assert_eq!(vm.fill_values.rhs, None, "rhs fill should be None");
    } else {
        panic!("Expected BinaryExpr, got {:?}", expr);
    }
}

#[test]
fn fill_right_modifier() {
    let input = "a * on(job) fill_right(100) b";
    let expr = parse_expr(input).expect("parse failed");

    if let Expr::Binary(BinaryExpr {
        vector_matching, ..
    }) = expr
    {
        let vm = vector_matching.expect("vector_matching should be present");
        assert!(vm.on, "on should be true");
        assert_eq!(vm.fill_values.lhs, None, "lhs fill should be None");
        assert_eq!(vm.fill_values.rhs, Some(100.0), "rhs fill should be 100");
    } else {
        panic!("Expected BinaryExpr, got {:?}", expr);
    }
}

#[test]
fn fill_left_right_modifier() {
    let input = "a / on(instance) fill_left(0) fill_right(1) b";
    let expr = parse_expr(input).expect("parse failed");

    if let Expr::Binary(BinaryExpr {
        vector_matching, ..
    }) = expr
    {
        let vm = vector_matching.expect("vector_matching should be present");
        assert!(vm.on, "on should be true");
        assert_eq!(vm.fill_values.lhs, Some(0.0), "lhs fill should be 0");
        assert_eq!(vm.fill_values.rhs, Some(1.0), "rhs fill should be 1");
    } else {
        panic!("Expected BinaryExpr, got {:?}", expr);
    }
}

#[test]
fn fill_right_left_modifier() {
    let input = "a - ignoring(pod) fill_right(5.5) fill_left(2.5) b";
    let expr = parse_expr(input).expect("parse failed");

    if let Expr::Binary(BinaryExpr {
        vector_matching, ..
    }) = expr
    {
        let vm = vector_matching.expect("vector_matching should be present");
        assert!(!vm.on, "on should be false");
        assert_eq!(vm.fill_values.lhs, Some(2.5), "lhs fill should be 2.5");
        assert_eq!(vm.fill_values.rhs, Some(5.5), "rhs fill should be 5.5");
    } else {
        panic!("Expected BinaryExpr, got {:?}", expr);
    }
}

#[test]
fn fill_with_negative_value() {
    let input = "a + on(instance) fill(-1) b";
    let expr = parse_expr(input).expect("parse failed");

    if let Expr::Binary(BinaryExpr {
        vector_matching, ..
    }) = expr
    {
        let vm = vector_matching.expect("vector_matching should be present");
        assert_eq!(vm.fill_values.lhs, Some(-1.0), "lhs fill should be -1");
        assert_eq!(vm.fill_values.rhs, Some(-1.0), "rhs fill should be -1");
    } else {
        panic!("Expected BinaryExpr, got {:?}", expr);
    }
}

#[test]
fn fill_with_group_left() {
    let input = "a + on(instance) group_left(pod) fill(0) b";
    let expr = parse_expr(input).expect("parse failed");

    if let Expr::Binary(BinaryExpr {
        vector_matching, ..
    }) = expr
    {
        let vm = vector_matching.expect("vector_matching should be present");
        assert!(vm.on, "on should be true");
        assert_eq!(vm.card, VectorMatchCardinality::ManyToOne);
        assert_eq!(vm.include, vec!["pod"]);
        assert_eq!(vm.fill_values.lhs, Some(0.0), "lhs fill should be 0");
        assert_eq!(vm.fill_values.rhs, Some(0.0), "rhs fill should be 0");
    } else {
        panic!("Expected BinaryExpr, got {:?}", expr);
    }
}

#[test]
fn complex_fill_chain() {
    let input = "a == bool on(instance, job) group_left(pod) fill_left(-1.5) fill_right(2.5) b";
    let expr = parse_expr(input).expect("parse failed");

    if let Expr::Binary(BinaryExpr {
        return_bool,
        vector_matching,
        ..
    }) = expr
    {
        assert!(return_bool, "bool flag should be set");
        let vm = vector_matching.expect("vector_matching should be present");
        assert!(vm.on, "on should be true");
        assert_eq!(vm.matching_labels, vec!["instance", "job"]);
        assert_eq!(vm.card, VectorMatchCardinality::ManyToOne);
        assert_eq!(vm.include, vec!["pod"]);
        assert_eq!(vm.fill_values.lhs, Some(-1.5), "lhs fill should be -1.5");
        assert_eq!(vm.fill_values.rhs, Some(2.5), "rhs fill should be 2.5");
    } else {
        panic!("Expected BinaryExpr, got {:?}", expr);
    }
}
