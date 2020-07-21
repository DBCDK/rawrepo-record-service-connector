/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GPLv3
 * See license text in LICENSE.txt or at https://opensource.dbc.dk/licenses/gpl-3.0/
 */

package dk.dbc.rawrepo.queue;

public class QueueServiceConnectorException extends Exception {
    public QueueServiceConnectorException(String msg) {
        super(msg);
    }
}
