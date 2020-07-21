/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GPLv3
 * See license text in LICENSE.txt or at https://opensource.dbc.dk/licenses/gpl-3.0/
 */

package dk.dbc.rawrepo.dto;

import java.util.List;

public class RecordHistoryCollection {

    private List<RecordHistory> recordHistoryList;

    public List<RecordHistory> getRecordHistoryList() {
        return recordHistoryList;
    }

    public void setRecordHistoryList(List<RecordHistory> recordHistoryList) {
        this.recordHistoryList = recordHistoryList;
    }
}
