UPDATE th409.purchase_issue SET updated_on = NOW(), invoice_sequence_type = 'DELIVERY_CHALLAN_NORMAL' WHERE id IN (9487,9501,9502,9503) AND invoice_sequence_type IS NULL;
UPDATE th409.purchase_issue SET updated_on = NOW(), invoice_sequence_type = 'DEBIT_NOTE_NUMBER' WHERE id IN (30602,30649,33743) AND invoice_sequence_type IS NULL;
