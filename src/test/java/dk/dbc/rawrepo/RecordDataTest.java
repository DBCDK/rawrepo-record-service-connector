/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GPLv3
 * See license text in LICENSE.txt or at https://opensource.dbc.dk/licenses/gpl-3.0/
 */

package dk.dbc.rawrepo;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class RecordDataTest {

    @Test
    void RecordIdEquals() {
        RecordData.RecordId recordId = new RecordData.RecordId("123456", 870970);

        assertThat(recordId, notNullValue());
        assertThat(recordId.equals(null), is(false));
        assertThat(recordId.equals(recordId), is(true));
        assertThat(recordId.equals(new RecordData.RecordId("123456", 870970)), is(true));
        assertThat(recordId.equals(new RecordData.RecordId("123456", 700000)), is(false));
        assertThat(recordId.equals(new RecordData.RecordId("654321", 870970)), is(false));
    }
}
