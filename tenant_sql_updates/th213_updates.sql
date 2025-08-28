UPDATE th213.purchase_issue SET updated_on = NOW(), invoice_sequence_type = 'DEBIT_NOTE_NUMBER' WHERE id IN (46643,46911,47984) AND invoice_sequence_type IS NULL;
