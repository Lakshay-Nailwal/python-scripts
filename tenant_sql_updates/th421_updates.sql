UPDATE th421.purchase_issue SET updated_on = NOW(), invoice_sequence_type = 'DEBIT_NOTE_NUMBER' WHERE id IN (16336,16337,16339) AND invoice_sequence_type IS NULL;
