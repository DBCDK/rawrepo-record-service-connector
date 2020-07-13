/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GPLv3
 * See license text in LICENSE.txt or at https://opensource.dbc.dk/licenses/gpl-3.0/
 */

package dk.dbc.rawrepo;

import java.util.Arrays;
import java.util.HashMap;
import java.util.stream.Collectors;

public class RecordDataCollection {

    RecordData[] records;

    public RecordData[] getRecords() {
        return records;
    }

    public HashMap<String, RecordData> toMap() {
        return Arrays.stream(records)
                .collect(
                        Collectors.toMap(
                                record -> record.getRecordId().getBibliographicRecordId(),
                                record -> record,
                                (first, second) -> second,
                                HashMap::new));
    }
}
