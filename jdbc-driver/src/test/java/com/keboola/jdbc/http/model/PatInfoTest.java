package com.keboola.jdbc.http.model;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PatInfoTest {

    @Test
    void constructor_exposesEveryField() {
        PatInfo pat = new PatInfo("pat-uuid", "my laptop", new PatInfo.Scope(true),
                Collections.singletonList(new PatInfo.ProjectAccess(7L, "Alpha", "admin")),
                true, "2026-12-31T23:59:59+00:00");

        assertEquals("pat-uuid", pat.getId());
        assertEquals("my laptop", pat.getName());
        assertTrue(pat.getScope().isAll());
        assertTrue(pat.isReadOnly());
        assertEquals("2026-12-31T23:59:59+00:00", pat.getExpiresAt());
        assertEquals(1, pat.getAccessibleProjects().size());
        assertEquals(7L, pat.getAccessibleProjects().get(0).getId());
        assertEquals("Alpha", pat.getAccessibleProjects().get(0).getName());
        assertEquals("admin", pat.getAccessibleProjects().get(0).getRole());
    }

    @Test
    void getAccessibleProjects_nullProjects_returnsEmptyList() {
        PatInfo pat = new PatInfo("pat-uuid", "name", null, null, false, null);

        assertTrue(pat.getAccessibleProjects().isEmpty());
    }

    @Test
    void getAccessibleProjects_isUnmodifiable() {
        PatInfo pat = new PatInfo("pat-uuid", "name", null,
                Arrays.asList(new PatInfo.ProjectAccess(1L, "Alpha", "admin")), false, null);

        List<PatInfo.ProjectAccess> projects = pat.getAccessibleProjects();

        assertThrows(UnsupportedOperationException.class,
                () -> projects.add(new PatInfo.ProjectAccess(2L, "Beta", "guest")));
    }
}
