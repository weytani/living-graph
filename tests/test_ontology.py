# ABOUTME: Tests for the ontology parser.
# ABOUTME: Verifies Convention/Ontology page is correctly parsed into TypeDef objects.

from living_graph.ontology import OntologyParser, TypeDef


def test_parse_returns_all_types(roam):
    parser = OntologyParser(roam)
    types = parser.parse()
    assert len(types) == 19
    assert all(isinstance(t, TypeDef) for t in types.values())


def test_person_type_has_correct_fields(roam):
    parser = OntologyParser(roam)
    types = parser.parse()
    person = types["Person"]
    assert person.namespace == "Person/"
    assert "Role" in person.required
    assert "Related" in person.required
    assert person.statuses == []


def test_project_type_has_statuses(roam):
    parser = OntologyParser(roam)
    types = parser.parse()
    project = types["Project"]
    assert "active" in project.statuses
    assert "paused" in project.statuses
    assert "completed" in project.statuses
    assert "archived" in project.statuses
    assert "Track" in project.required
    assert "Status" in project.required


def test_task_type_has_statuses(roam):
    parser = OntologyParser(roam)
    types = parser.parse()
    task = types["Task"]
    assert "todo" in task.statuses
    assert "active" in task.statuses
    assert "blocked" in task.statuses
    assert "done" in task.statuses


def test_type_lookup_by_namespace(roam):
    parser = OntologyParser(roam)
    types = parser.parse()
    person = parser.type_for_namespace("Person/")
    assert person is not None
    assert person.name == "Person"


def test_epistemic_types_present(roam):
    parser = OntologyParser(roam)
    types = parser.parse()
    epistemic = ["Assumption", "Constraint", "Contradiction", "Synthesis", "Decision"]
    for name in epistemic:
        assert name in types, f"Missing epistemic type: {name}"
