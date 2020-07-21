/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GPLv3
 * See license text in LICENSE.txt or at https://opensource.dbc.dk/licenses/gpl-3.0/
 */

package dk.dbc.rawrepo;

import dk.dbc.rawrepo.dto.RecordIdDTO;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class RecordDataTest {

    @Test
    void RecordIdEquals() {
        RecordIdDTO recordId = new RecordIdDTO("123456", 870970);

        assertThat(recordId, notNullValue());
        assertThat(recordId.equals(null), is(false));
        assertThat(recordId.equals(recordId), is(true));
        assertThat(recordId.equals(new RecordIdDTO("123456", 870970)), is(true));
        assertThat(recordId.equals(new RecordIdDTO("123456", 700000)), is(false));
        assertThat(recordId.equals(new RecordIdDTO("654321", 870970)), is(false));
    }

    @Test
    void RecordIdHashCode() {
        RecordIdDTO recordId_1 = new RecordIdDTO("123456", 870970);
        RecordIdDTO recordId_2 = new RecordIdDTO("123456", 870970);
        RecordIdDTO recordId_3 = new RecordIdDTO("654321", 870970);
        RecordIdDTO recordId_4 = new RecordIdDTO("123456", 870971);

        assertThat(recordId_1.hashCode(), is(recordId_1.hashCode()));
        assertThat(recordId_1.hashCode(), is(recordId_2.hashCode()));

        assertThat(recordId_1.hashCode(), not(recordId_3.hashCode()));
        assertThat(recordId_1.hashCode(), not(recordId_4.hashCode()));
        assertThat(recordId_3.hashCode(), not(recordId_4.hashCode()));
    }
}
