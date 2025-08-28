UPDATE th999.purchase_issue SET updated_on = NOW(), invoice_sequence_type = 'DELIVERY_CHALLAN_NORMAL' WHERE id IN (201,202,203,204,205,206,207,216,222,226,227) AND invoice_sequence_type IS NULL;
