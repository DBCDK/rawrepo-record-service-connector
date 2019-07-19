/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GPLv3
 * See license text in LICENSE.txt or at https://opensource.dbc.dk/licenses/gpl-3.0/
 */

package dk.dbc.rawrepo;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class RecordDataTest {

    @Test
    void RecordIdEquals() {
        RecordId recordId = new RecordId("123456", 870970);

        assertThat(recordId, notNullValue());
        assertThat(recordId.equals(null), is(false));
        assertThat(recordId.equals(recordId), is(true));
        assertThat(recordId.equals(new RecordId("123456", 870970)), is(true));
        assertThat(recordId.equals(new RecordId("123456", 700000)), is(false));
        assertThat(recordId.equals(new RecordId("654321", 870970)), is(false));
    }

    @Test
    void RecordIdHashCode() {
        RecordId recordId_1 = new RecordId("123456", 870970);
        RecordId recordId_2 = new RecordId("123456", 870970);
        RecordId recordId_3 = new RecordId("654321", 870970);
        RecordId recordId_4 = new RecordId("123456", 870971);

        assertThat(recordId_1.hashCode(), is(recordId_1.hashCode()));
        assertThat(recordId_1.hashCode(), is(recordId_2.hashCode()));

        assertThat(recordId_1.hashCode(), not(recordId_3.hashCode()));
        assertThat(recordId_1.hashCode(), not(recordId_4.hashCode()));
        assertThat(recordId_3.hashCode(), not(recordId_4.hashCode()));
    }
}
