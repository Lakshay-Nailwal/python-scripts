UPDATE th434.purchase_issue SET updated_on = NOW(), invoice_sequence_type = 'DEBIT_NOTE_NUMBER' WHERE id IN (1,2) AND invoice_sequence_type IS NULL;
UPDATE th434.purchase_issue SET updated_on = NOW(), invoice_sequence_type = 'DELIVERY_CHALLAN_NORMAL' WHERE id IN (244) AND invoice_sequence_type IS NULL;
