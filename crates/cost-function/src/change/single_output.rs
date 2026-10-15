use std::convert::Infallible;

use crate::{
    change::{ChangeSelection, ChangeSelector},
    selection::InputSelection,
};

/// Returns the whole surplus as one change output.
pub(crate) struct SingleOutputChangeSelector;

impl ChangeSelector for SingleOutputChangeSelector {
    fn select(&self, request: &InputSelection) -> Result<ChangeSelection, Self::Error> {
        Ok(ChangeSelection(vec![request.surplus]))
    }

    type Error = Infallible;
}

#[cfg(test)]
mod tests {
    use bitcoin::Amount;

    use super::*;

    fn selection(surplus: u64) -> InputSelection {
        InputSelection {
            selected_inputs: vec![],
            surplus: Amount::from_sat(surplus),
        }
    }

    #[test]
    fn the_whole_surplus_becomes_one_change_output() {
        let change = SingleOutputChangeSelector.select(&selection(546)).unwrap();

        assert_eq!(change, ChangeSelection(vec![Amount::from_sat(546)]));
    }
}
